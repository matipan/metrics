// Package beego provides a reporter that wraps our datadog
// client with metrics for reporting request counts, error
// rates and request durations. It does this by providing
// a set of filters that can be used when initializing your
// router.
package beego

import (
	stdcontext "context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/context"
	"github.com/spiritclips/entitlements-go/metrics"
	"github.com/spiritclips/entitlements-go/metrics/datadog"
)

const (
	countRequestsMetric = "request_count"
	count500Metric      = "request_500"
	count404Metric      = "resquest_404"
)

// Reporter reports events retrieved from the lifecycle
// of requests handled by beego.
type Reporter struct {
	client *datadog.Client

	muRequests      sync.Mutex
	requestCounters map[string]metrics.Counter

	muDurations      sync.Mutex
	requestDurations map[string]metrics.Histogram
}

// New creates a new beego reporter.
func New(client *datadog.Client) *Reporter {
	return &Reporter{
		client:           client,
		requestCounters:  make(map[string]metrics.Counter),
		requestDurations: make(map[string]metrics.Histogram),
	}
}

// ReportLoop reports the metrics that happen
// each time the ch sends an event.
// This is a blocking function.
func (reporter *Reporter) ReportLoop(ctx stdcontext.Context, ch <-chan time.Time) {
	reporter.client.ReportLoop(ctx, ch)
}

// ErrCounter500 counts the 500 errors.
func (reporter *Reporter) ErrCounter500() beego.FilterFunc {
	return func(ctx *context.Context) {
		if ctx.ResponseWriter.Status != http.StatusInternalServerError {
			return
		}
		reporter.requestCounter(count500Metric, getEndpoint(ctx)).Add(1)
	}
}

// ErrCounter404 counts 404 errors.
func (reporter *Reporter) ErrCounter404() beego.FilterFunc {
	return func(ctx *context.Context) {
		if ctx.ResponseWriter.Status != http.StatusNotFound {
			return
		}
		reporter.requestCounter(count404Metric, getEndpoint(ctx)).Add(1)
	}
}

// RequestCounter is a beego filter that counts the requests
// that come in for each endpoint. If `countErrors` is true
// it also registers error counters for 500 and 404 errors.
// Sending countErrors true is a shortcut for registering
// ErrCounter500 and ErrCounter404. If you only want one of them
// then send `countErrors` as false and register the one
// you are interested in.
func (reporter *Reporter) RequestCounter(countErrors bool) beego.FilterFunc {
	return func(ctx *context.Context) {
		endpoint := getEndpoint(ctx)
		counter := reporter.requestCounter(countRequestsMetric, endpoint)
		counter.Add(1)
		if !countErrors {
			return
		}
		reporter.requestCounter(count500Metric, endpoint).Add(1)
		switch ctx.ResponseWriter.Status {
		case http.StatusInternalServerError:
			reporter.requestCounter(count500Metric, endpoint).Add(1)
		case http.StatusNotFound:
			reporter.requestCounter(count404Metric, endpoint).Add(1)
		}
	}
}

// RequestDuration measures the duration of a request. In order
// to measure how long it took we first need to insert the time when
// the request was received. This is why the function returns two filters,
// one has to be used as a `beego.BeforeRouter` and the second one
// as a `beego.AfterExec`.
func (reporter *Reporter) RequestDuration() (before, after beego.FilterFunc) {
	return func(ctx *context.Context) {
			ctx.Input.SetData("requestExecTimestamp", time.Now().Unix())
		}, func(ctx *context.Context) {
			timestamp := ctx.Input.GetData("requestExecTimestamp").(int64)
			seconds := time.Now().Unix() - timestamp
			reporter.requestDuration(getEndpoint(ctx)).Observe(float64(seconds * 1000))
		}
}

// requestCounter fetches a counter using the metric name and
// the corresponding endpoint as key. If the counter does not exist then
// we create a new one, the key for the counter is the metricName
// appended by the endpoint.
func (reporter *Reporter) requestCounter(metricName, endpoint string) metrics.Counter {
	reporter.muRequests.Lock()
	defer reporter.muRequests.Unlock()
	counter, ok := reporter.requestCounters[metricName+endpoint]
	if !ok {
		reporter.requestCounters[metricName+endpoint] = reporter.client.NewCounter(fmt.Sprintf("entitlements.%s", metricName)).
			With("environment", beego.BConfig.RunMode, "request", endpoint)
		return reporter.requestCounters[metricName+endpoint]
	}
	return counter
}

// requestDuration fetches a histogram using the endoint
// as key. If the histogram does not exist then it creates
// a new one.
func (reporter *Reporter) requestDuration(endpoint string) metrics.Histogram {
	reporter.muDurations.Lock()
	defer reporter.muDurations.Unlock()
	duration, ok := reporter.requestDurations[endpoint]
	if !ok {
		reporter.requestDurations[endpoint] = reporter.client.NewHistogram("entitlements.request_duration", "millisecond").
			With("environment", beego.BConfig.RunMode, "request", endpoint)
		return reporter.requestDurations[endpoint]
	}
	return duration
}

// getEndpoint returns the endpoint of this context
// mapped by beego. It does not return the path of
// the request, simply the endpoint with which the
// path matches. For example if the path of this
// request is: `/entitlements/subscription/some_vertical_code`
// the corresponding endpoint is: `/entitlements/subscription/:vertical_code`.
func getEndpoint(ctx *context.Context) string {
	return ctx.Input.GetData("RouterPattern").(string)
}
