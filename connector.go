// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package histogramconnector

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
)

const scopeName = "otelcol/histogramconnector"

// hgram can count spans, span event, metrics, data points, or log records
// and emit the counts onto a metrics pipeline.
type hgram struct {
	metricsConsumer consumer.Metrics
	component.StartFunc
	component.ShutdownFunc

	spansMetricDefs      map[string]metricDef[ottlspan.TransformContext]
	spanEventsMetricDefs map[string]metricDef[ottlspanevent.TransformContext]
	metricsMetricDefs    map[string]metricDef[ottlmetric.TransformContext]
	dataPointsMetricDefs map[string]metricDef[ottldatapoint.TransformContext]
	logsMetricDefs       map[string]metricDef[ottllog.TransformContext]
}

func (c *hgram) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *hgram) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var multiError error
	hgramMetrics := pmetric.NewMetrics()
	hgramMetrics.ResourceMetrics().EnsureCapacity(td.ResourceSpans().Len())
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		spansCounter := newCounter[ottlspan.TransformContext](c.spansMetricDefs)
		spanEventsCounter := newCounter[ottlspanevent.TransformContext](c.spanEventsMetricDefs)

		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)

			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				sCtx := ottlspan.NewTransformContext(span, scopeSpan.Scope(), resourceSpan.Resource())
				multiError = errors.Join(multiError, spansCounter.update(ctx, span.Attributes(), sCtx))

				for l := 0; l < span.Events().Len(); l++ {
					event := span.Events().At(l)
					eCtx := ottlspanevent.NewTransformContext(event, span, scopeSpan.Scope(), resourceSpan.Resource())
					multiError = errors.Join(multiError, spanEventsCounter.update(ctx, event.Attributes(), eCtx))
				}
			}
		}

		if len(spansCounter.counts)+len(spanEventsCounter.counts) == 0 {
			continue // don't add an empty resource
		}

		hgramResource := hgramMetrics.ResourceMetrics().AppendEmpty()
		resourceSpan.Resource().Attributes().CopyTo(hgramResource.Resource().Attributes())

		hgramResource.ScopeMetrics().EnsureCapacity(resourceSpan.ScopeSpans().Len())
		hgramScope := hgramResource.ScopeMetrics().AppendEmpty()
		hgramScope.Scope().SetName(scopeName)

		spansCounter.appendMetricsTo(hgramScope.Metrics())
		spanEventsCounter.appendMetricsTo(hgramScope.Metrics())
	}
	if multiError != nil {
		return multiError
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, hgramMetrics)
}

func (c *hgram) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	var multiError error
	hgramMetrics := pmetric.NewMetrics()
	hgramMetrics.ResourceMetrics().EnsureCapacity(md.ResourceMetrics().Len())
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)
		metricsCounter := newCounter[ottlmetric.TransformContext](c.metricsMetricDefs)
		dataPointsCounter := newCounter[ottldatapoint.TransformContext](c.dataPointsMetricDefs)

		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetric.ScopeMetrics().At(j)

			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)
				mCtx := ottlmetric.NewTransformContext(metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), resourceMetric.Resource())
				multiError = errors.Join(multiError, metricsCounter.update(ctx, pcommon.NewMap(), mCtx))

				//exhaustive:enforce
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dCtx := ottldatapoint.NewTransformContext(dps.At(i), metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), resourceMetric.Resource())
						multiError = errors.Join(multiError, dataPointsCounter.update(ctx, dps.At(i).Attributes(), dCtx))
					}
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dCtx := ottldatapoint.NewTransformContext(dps.At(i), metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), resourceMetric.Resource())
						multiError = errors.Join(multiError, dataPointsCounter.update(ctx, dps.At(i).Attributes(), dCtx))
					}
				case pmetric.MetricTypeSummary:
					dps := metric.Summary().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dCtx := ottldatapoint.NewTransformContext(dps.At(i), metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), resourceMetric.Resource())
						multiError = errors.Join(multiError, dataPointsCounter.update(ctx, dps.At(i).Attributes(), dCtx))
					}
				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dCtx := ottldatapoint.NewTransformContext(dps.At(i), metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), resourceMetric.Resource())
						multiError = errors.Join(multiError, dataPointsCounter.update(ctx, dps.At(i).Attributes(), dCtx))
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := metric.ExponentialHistogram().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dCtx := ottldatapoint.NewTransformContext(dps.At(i), metric, scopeMetrics.Metrics(), scopeMetrics.Scope(), resourceMetric.Resource())
						multiError = errors.Join(multiError, dataPointsCounter.update(ctx, dps.At(i).Attributes(), dCtx))
					}
				case pmetric.MetricTypeEmpty:
					multiError = errors.Join(multiError, fmt.Errorf("metric %q: invalid metric type: %v", metric.Name(), metric.Type()))
				}
			}
		}

		if len(metricsCounter.counts)+len(dataPointsCounter.counts) == 0 {
			continue // don't add an empty resource
		}

		hgramResource := hgramMetrics.ResourceMetrics().AppendEmpty()
		resourceMetric.Resource().Attributes().CopyTo(hgramResource.Resource().Attributes())

		hgramResource.ScopeMetrics().EnsureCapacity(resourceMetric.ScopeMetrics().Len())
		hgramScope := hgramResource.ScopeMetrics().AppendEmpty()
		hgramScope.Scope().SetName(scopeName)

		metricsCounter.appendMetricsTo(hgramScope.Metrics())
		dataPointsCounter.appendMetricsTo(hgramScope.Metrics())
	}
	if multiError != nil {
		return multiError
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, hgramMetrics)
}

func (c *hgram) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	var multiError error
	hgramMetrics := pmetric.NewMetrics()
	hgramMetrics.ResourceMetrics().EnsureCapacity(ld.ResourceLogs().Len())
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLog := ld.ResourceLogs().At(i)
		hgram := newCounter[ottllog.TransformContext](c.logsMetricDefs)

		for j := 0; j < resourceLog.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLog.ScopeLogs().At(j)

			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				logRecord := scopeLogs.LogRecords().At(k)

				lCtx := ottllog.NewTransformContext(logRecord, scopeLogs.Scope(), resourceLog.Resource())
				multiError = errors.Join(multiError, hgram.update(ctx, logRecord.Attributes(), lCtx))
			}
		}

		if len(hgram.counts) == 0 {
			continue // don't add an empty resource
		}

		hgramResource := hgramMetrics.ResourceMetrics().AppendEmpty()
		resourceLog.Resource().Attributes().CopyTo(hgramResource.Resource().Attributes())

		hgramResource.ScopeMetrics().EnsureCapacity(resourceLog.ScopeLogs().Len())
		hgramScope := hgramResource.ScopeMetrics().AppendEmpty()
		hgramScope.Scope().SetName(scopeName)

		hgram.appendMetricsTo(hgramScope.Metrics())
	}
	if multiError != nil {
		return multiError
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, hgramMetrics)
}
