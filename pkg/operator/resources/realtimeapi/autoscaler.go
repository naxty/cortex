/*
Copyright 2021 Cortex Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package realtimeapi

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/cortexlabs/cortex/pkg/lib/errors"
	libmath "github.com/cortexlabs/cortex/pkg/lib/math"
	s "github.com/cortexlabs/cortex/pkg/lib/strings"
	libtime "github.com/cortexlabs/cortex/pkg/lib/time"
	"github.com/cortexlabs/cortex/pkg/operator/config"
	"github.com/cortexlabs/cortex/pkg/types/userconfig"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	kapps "k8s.io/api/apps/v1"
)

const (
	_inFlightRequestsPrometheusKey = "cortex_in_flight_requests"
	_prometheusQueryTimeoutSeconds = 10
)

type recommendations map[time.Time]int32

func (recs recommendations) add(rec int32) {
	recs[time.Now()] = rec
}

func (recs recommendations) deleteOlderThan(period time.Duration) {
	for t := range recs {
		if time.Since(t) > period {
			delete(recs, t)
		}
	}
}

// Returns nil if no recommendations in the period
func (recs recommendations) maxSince(period time.Duration) *int32 {
	max := int32(math.MinInt32)
	foundRecommendation := false

	for t, rec := range recs {
		if time.Since(t) <= period && rec > max {
			max = rec
			foundRecommendation = true
		}
	}

	if !foundRecommendation {
		return nil
	}

	return &max
}

// Returns nil if no recommendations in the period
func (recs recommendations) minSince(period time.Duration) *int32 {
	min := int32(math.MaxInt32)
	foundRecommendation := false

	for t, rec := range recs {
		if time.Since(t) <= period && rec < min {
			min = rec
			foundRecommendation = true
		}
	}

	if !foundRecommendation {
		return nil
	}

	return &min
}

func autoscaleFn(initialDeployment *kapps.Deployment) (func() error, error) {
	autoscalingSpec, err := userconfig.AutoscalingFromAnnotations(initialDeployment)
	if err != nil {
		return nil, err
	}

	apiName := initialDeployment.Labels["apiName"]
	currentReplicas := *initialDeployment.Spec.Replicas

	log.Printf("%s autoscaler init", apiName)

	var startTime time.Time
	recs := make(recommendations)

	return func() error {
		if startTime.IsZero() {
			startTime = time.Now()
		}

		avgInFlight, err := getInflightRequests(apiName, autoscalingSpec.Window)
		if err != nil {
			return err
		}
		if avgInFlight == nil {
			log.Printf("%s autoscaler tick: metrics not available yet", apiName)
			return nil
		}

		rawRecommendation := *avgInFlight / *autoscalingSpec.TargetReplicaConcurrency
		recommendation := int32(math.Ceil(rawRecommendation))

		if rawRecommendation < float64(currentReplicas) && rawRecommendation > float64(currentReplicas)*(1-autoscalingSpec.DownscaleTolerance) {
			recommendation = currentReplicas
		}

		if rawRecommendation > float64(currentReplicas) && rawRecommendation < float64(currentReplicas)*(1+autoscalingSpec.UpscaleTolerance) {
			recommendation = currentReplicas
		}

		// always allow subtraction of 1
		downscaleFactorFloor := libmath.MinInt32(currentReplicas-1, int32(math.Ceil(float64(currentReplicas)*autoscalingSpec.MaxDownscaleFactor)))
		if recommendation < downscaleFactorFloor {
			recommendation = downscaleFactorFloor
		}

		// always allow addition of 1
		upscaleFactorCeil := libmath.MaxInt32(currentReplicas+1, int32(math.Ceil(float64(currentReplicas)*autoscalingSpec.MaxUpscaleFactor)))
		if recommendation > upscaleFactorCeil {
			recommendation = upscaleFactorCeil
		}

		if recommendation < 1 {
			recommendation = 1
		}

		if recommendation < autoscalingSpec.MinReplicas {
			recommendation = autoscalingSpec.MinReplicas
		}

		if recommendation > autoscalingSpec.MaxReplicas {
			recommendation = autoscalingSpec.MaxReplicas
		}

		// Rule of thumb: any modifications that don't consider historical recommendations should be performed before
		// recording the recommendation, any modifications that use historical recommendations should be performed after
		recs.add(recommendation)

		// This is just for garbage collection
		recs.deleteOlderThan(libtime.MaxDuration(autoscalingSpec.DownscaleStabilizationPeriod, autoscalingSpec.UpscaleStabilizationPeriod))

		request := recommendation

		downscaleStabilizationFloor := recs.maxSince(autoscalingSpec.DownscaleStabilizationPeriod)
		if time.Since(startTime) < autoscalingSpec.DownscaleStabilizationPeriod {
			if request < currentReplicas {
				request = currentReplicas
			}
		} else if downscaleStabilizationFloor != nil && request < *downscaleStabilizationFloor {
			request = *downscaleStabilizationFloor
		}

		upscaleStabilizationCeil := recs.minSince(autoscalingSpec.UpscaleStabilizationPeriod)
		if time.Since(startTime) < autoscalingSpec.UpscaleStabilizationPeriod {
			if request > currentReplicas {
				request = currentReplicas
			}
		} else if upscaleStabilizationCeil != nil && request > *upscaleStabilizationCeil {
			request = *upscaleStabilizationCeil
		}

		log.Printf("%s autoscaler tick: avg_in_flight=%s, target_replica_concurrency=%s, raw_recommendation=%s, current_replicas=%d, downscale_tolerance=%s, upscale_tolerance=%s, max_downscale_factor=%s, downscale_factor_floor=%d, max_upscale_factor=%s, upscale_factor_ceil=%d, min_replicas=%d, max_replicas=%d, recommendation=%d, downscale_stabilization_period=%s, downscale_stabilization_floor=%s, upscale_stabilization_period=%s, upscale_stabilization_ceil=%s, request=%d", apiName, s.Round(*avgInFlight, 2, 0), s.Float64(*autoscalingSpec.TargetReplicaConcurrency), s.Round(rawRecommendation, 2, 0), currentReplicas, s.Float64(autoscalingSpec.DownscaleTolerance), s.Float64(autoscalingSpec.UpscaleTolerance), s.Float64(autoscalingSpec.MaxDownscaleFactor), downscaleFactorFloor, s.Float64(autoscalingSpec.MaxUpscaleFactor), upscaleFactorCeil, autoscalingSpec.MinReplicas, autoscalingSpec.MaxReplicas, recommendation, autoscalingSpec.DownscaleStabilizationPeriod, s.ObjFlatNoQuotes(downscaleStabilizationFloor), autoscalingSpec.UpscaleStabilizationPeriod, s.ObjFlatNoQuotes(upscaleStabilizationCeil), request)

		if currentReplicas != request {
			log.Printf("%s autoscaling event: %d -> %d", apiName, currentReplicas, request)

			deployment, err := config.K8s.GetDeployment(initialDeployment.Name)
			if err != nil {
				return err
			}

			if deployment == nil {
				return errors.ErrorUnexpected("unable to find k8s deployment", apiName)
			}

			deployment.Spec.Replicas = &request

			if _, err := config.K8s.UpdateDeployment(deployment); err != nil {
				return err
			}

			currentReplicas = request
		}

		return nil
	}, nil
}

func getInflightRequests(apiName string, window time.Duration) (*float64, error) {
	promClient, err := promapi.NewClient(promapi.Config{
		Address: config.Cluster.PrometheusURL,
	})

	promAPIv1 := promv1.NewAPI(promClient)
	windowSeconds := int64(window.Truncate(time.Second).Seconds())

	// PromQL query:
	// 	sum(sum_over_time(cortex_in_flight_requests{api_name="<apiName>"}[60s])) /
	//	sum(count_over_time(cortex_in_flight_requests{api_name="<apiName>"}[60s]))
	query := fmt.Sprintf(
		"sum(sum_over_time(%s{api_name=\"%s\"}[%ds])) / sum(count_over_time(%s{api_name=\"%s\"}[%ds]))",
		_inFlightRequestsPrometheusKey, apiName, windowSeconds,
		_inFlightRequestsPrometheusKey, apiName, windowSeconds,
	)

	ctx, cancel := context.WithTimeout(context.Background(), _prometheusQueryTimeoutSeconds*time.Second)
	defer cancel()

	valuesQuery, err := promAPIv1.Query(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}

	values, ok := valuesQuery.(model.Vector)
	if !ok {
		return nil, errors.ErrorUnexpected("failed to convert prometheus metric to vector")
	}

	// no values available
	if values.Len() == 0 {
		return nil, nil
	}

	avgInflightRequests := float64(values[0].Value)

	return &avgInflightRequests, nil
}
