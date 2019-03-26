// Package datadog provides a wrapper of the datadog client
// library with our own metrics interfaces.
package datadog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/heptio/workgroup"
	"github.com/matipan/metrics"
	datadog "gopkg.in/zorkian/go-datadog-api.v2"
)

const (
	counterType   = "count"
	histogramType = "rate"
)

// Client is a datadog client that reports
// metrics to it in a given interval.
type Client struct {
	muCounters   sync.Mutex
	counters     []*Counter
	muHistograms sync.Mutex
	histograms   []*Histogram
	muEvents     sync.Mutex
	events       []*Event

	client *datadog.Client
	logger *logrus.Entry
	host   string
}

// NewClient creates a new datadog client.
func NewClient(apiKey, appKey, host string) *Client {
	return &Client{
		counters:   []*Counter{},
		histograms: []*Histogram{},
		events:     []*Event{},
		client:     datadog.NewClient(apiKey, appKey),
		host:       host,
		logger:     logrus.WithField("logger", "datadog"),
	}
}

// NewCounter returns a metrics counter.
func (c *Client) NewCounter(name string) *Counter {
	c.muCounters.Lock()
	defer c.muCounters.Unlock()

	counter := &Counter{
		metric: &metric{
			name:       name,
			metricType: counterType,
			tags:       []string{},
			datapoints: []datadog.DataPoint{},
		},
	}
	c.counters = append(c.counters, counter)
	return counter
}

// NewHistogram creates a new histogram with the specified name.
func (c *Client) NewHistogram(name, unit string) *Histogram {
	c.muHistograms.Lock()
	defer c.muHistograms.Unlock()

	histogram := &Histogram{
		metric: &metric{
			name:       name,
			metricType: histogramType,
			tags:       []string{},
			datapoints: []datadog.DataPoint{},
		},
		unit: unit,
	}
	c.histograms = append(c.histograms, histogram)
	return histogram
}

// NewEvent creates a new event with the specified title, priority
// and alertType.
func (c *Client) NewEvent(title, priority, alertType string) *Event {
	c.muEvents.Lock()
	defer c.muEvents.Unlock()

	event := &Event{
		title:     title,
		priority:  priority,
		alertType: alertType,
		tags:      []string{},
		times:     []int{},
	}
	c.events = append(c.events, event)
	return event
}

// Report reports the current metrics and events to datadog.
// If the report is successful we delete the datapoints
// of each metric and timestamps of each event.
func (c *Client) Report() error {
	var g workgroup.Group
	g.Add(func(_ <-chan struct{}) error {
		c.muEvents.Lock()
		for k, event := range c.events {
			if len(event.times) == 0 {
				continue
			}
			// each event will have an array of timestamps of when that
			// particular event happened. The api has no way of doing a
			// batch request so we have to do one by one.
			for _, event := range event.Events() {
				if _, err := c.client.PostEvent(&event); err != nil {
					c.logger.Debugf("Unable to post event %s: %s", *event.Title, err)
				}
			}
			// clear all the times of the current event, we might loose
			// some events on the way but it's not something we should worry
			// about.
			c.events[k].mu.Lock()
			c.events[k].times = []int{}
			c.events[k].mu.Unlock()
		}
		c.muEvents.Unlock()
		return nil
	})
	g.Add(func(_ <-chan struct{}) error {
		metrics := []datadog.Metric{}
		postedCounters := []*Counter{}
		c.muCounters.Lock()
		// build an array of all the counters that need
		// to be posted as metrics to datadog.
		for k, counter := range c.counters {
			if len(counter.datapoints) > 0 {
				metrics = append(metrics, counter.Metric(c.host))
				// save all the counters that will be posted since
				// we will remove the datapoints for each of them later
				postedCounters = append(postedCounters, c.counters[k])
			}
		}
		c.muCounters.Unlock()

		if err := c.client.PostMetrics(metrics); err != nil {
			c.logger.Debugf("Unable to post counters: %s", err)
			return err
		}

		// remove the datapoints for each of the counters that
		// was posted so that they don't get posted again in the
		// future. This might cause us to loose a bit of information
		// but it's not something we should worry about.
		c.muCounters.Lock()
		for _, counter := range postedCounters {
			counter.mu.Lock()
			counter.datapoints = []datadog.DataPoint{}
			counter.mu.Unlock()
		}
		c.muCounters.Unlock()

		return nil
	})
	g.Add(func(_ <-chan struct{}) error {
		metrics := []datadog.Metric{}
		postedHistograms := []*Histogram{}
		c.muHistograms.Lock()
		// build an array of all the histograms that need
		// to be posted as metrics to datadog.
		for k, histogram := range c.histograms {
			if len(histogram.datapoints) > 0 {
				metrics = append(metrics, histogram.Metric(c.host))
				// save all the histogram that will be posted since
				// we will remove the datapoints for each of them later
				postedHistograms = append(postedHistograms, c.histograms[k])
			}
		}
		c.muHistograms.Unlock()

		if err := c.client.PostMetrics(metrics); err != nil {
			c.logger.Debugf("unable to post histograms: %s", err)
			return err
		}

		// remove the datapoints for each of the histograms that
		// was posted so that they don't get posted again in the
		// future. This might cause us to loose a bit of information
		// but it's not something we should worry about.
		c.muHistograms.Lock()
		for _, histogram := range postedHistograms {
			histogram.mu.Lock()
			histogram.datapoints = []datadog.DataPoint{}
			histogram.mu.Unlock()
		}
		c.muHistograms.Unlock()

		return nil
	})
	return g.Run()
}

// ReportLoop reports the metrics gathered so far to the
// datadog client every time we receive a message on the
// channel. You'd typically use this with a time.Ticker.
// This is a blocking function. In order to do a graceful
// shutdown send a context with a cancel function or a
// timeout.
func (c *Client) ReportLoop(ctx context.Context, ch <-chan time.Time) {
	for {
		select {
		case <-ch:
			if err := c.Report(); err != nil {
				c.logger.Debugf("Reporting metrics failed: %s", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// metric holds the basic fields that are shared
// by a histogram and a counter. Essentially they
// are the same thing, the only thing that changes
// is the metric type and the meaning that datadog
// gives to the datapoints.
type metric struct {
	mu         sync.RWMutex
	name       string
	metricType string
	tags       []string
	datapoints []datadog.DataPoint
}

// addLabelValues adds the list key-value pair as tags
// to the metric. For example:
// 	addLabelValues("environment", "dev")
// Will result in the tag: `environment:dev` when parsing
// this metric to a datadog metric.
func (m *metric) addLabelValues(labelValues ...string) *metric {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, "unknown")
	}
	m.tags = append(m.tags, labelValues...)
	return m
}

// addDelta adds the delta to the datapoints of this
// metric.
func (m *metric) addDelta(delta float64) *metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datapoints = append(m.datapoints, datadog.DataPoint{
		float64ptr(float64(time.Now().Unix())),
		&delta,
	})
	return m
}

// Metric uses the information of our own metric to build
// a datadog metric, we call this function before sending
// the metrics to the datadog api. See the `Report` function.
func (m *metric) Metric(host string) datadog.Metric {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return datadog.Metric{
		Metric: &m.name,
		Tags:   buildDatadogTags(m.tags),
		Points: m.datapoints,
		Type:   &m.metricType,
		Host:   &host,
	}
}

// Counter implements the metrics.Counter interface
// for datadog counters.
type Counter struct {
	*metric
}

// With returns the counter with the labelValues
// added as key-value pair tags.
func (c *Counter) With(labelValues ...string) metrics.Counter {
	c.metric = c.metric.addLabelValues(labelValues...)
	return c
}

// Add adds the new value to the counter.
func (c *Counter) Add(delta float64) {
	c.metric = c.addDelta(delta)
}

// Histogram implements the metric.Histogram interface
// for datadog histograms.
type Histogram struct {
	*metric
	unit string
}

// With returns the histogram with the labelValues
// added as key-value pair tags.
func (h *Histogram) With(labelValues ...string) metrics.Histogram {
	h.metric = h.metric.addLabelValues(labelValues...)
	return h
}

// Observe observes a new delta in the histogram.
func (h *Histogram) Observe(delta float64) {
	h.metric = h.metric.addDelta(delta)
}

// Metric converts this histogram into a datadog metric.
func (h *Histogram) Metric(host string) datadog.Metric {
	metric := h.metric.Metric(host)
	metric.Unit = &h.unit
	return metric
}

// Event implements the metrics.Event interface
// for datadog events.
type Event struct {
	title     string
	priority  string
	alertType string
	tags      []string

	mu    sync.RWMutex
	times []int
}

// With adds the labelValues as key-pair tags
// to the event.
func (e *Event) With(labelValues ...string) metrics.Event {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, "unknown")
	}
	e.tags = append(e.tags, labelValues...)
	return e
}

// Register registers a new event with the current timestamp.
func (e *Event) Register() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.times = append(e.times, int(time.Now().Unix()))
}

// Events returns datadog events for each the times
// that this event was reported.
func (e *Event) Events() []datadog.Event {
	e.mu.RLock()
	defer e.mu.RUnlock()
	events := []datadog.Event{}
	for _, t := range e.times {
		events = append(events, datadog.Event{
			Title:     &e.title,
			Priority:  &e.priority,
			AlertType: &e.alertType,
			Tags:      buildDatadogTags(e.tags),
			Time:      &t,
		})
	}
	return events
}

func buildDatadogTags(labelValues []string) []string {
	tags := []string{}
	for i := 0; i < len(labelValues); i += 2 {
		tags = append(tags, fmt.Sprintf("%s:%s", labelValues[i], labelValues[i+1]))
	}
	return tags
}

// float64ptr returns a pointer to the argument. This is used
// in `metric.Metric` to set the fields of a datadog metric.
func float64ptr(d float64) *float64 {
	return &d
}
