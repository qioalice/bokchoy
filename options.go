//
// ORIGINAL PACKAGE
// ( https://github.com/thoas/bokchoy )
//
//     Copyright © 2019. All rights reserved.
//     Author: Florent Messa
//     Contacts: florent.messa@gmail.com, https://github.com/thoas
//     License: https://opensource.org/licenses/MIT
//
// HAS BEEN FORKED, HIGHLY MODIFIED AND NOW IS AVAILABLE AS
// ( https://github.com/qioalice/bokchoy )
//
//     Copyright © 2020. All rights reserved.
//     Author: Ilya Stroy.
//     Contacts: qioalice@gmail.com, https://github.com/qioalice
//     License: https://opensource.org/licenses/MIT
//

package bokchoy

import (
	"strings"
	"time"

	"github.com/qioalice/ekago/v2/ekalog"
)

// Options is the bokchoy options.
type Options struct {

	// --- Required options ---

	Broker            Broker
	Serializer        Serializer

	// --- Additional options ---

	Logger            *ekalog.Logger
	loggerIsPresented bool

	Concurrency       int
	MaxRetries        int
	TTL               time.Duration
	Countdown         *time.Duration
	Timeout           time.Duration
	RetryIntervals    []time.Duration
	Initialize        bool
	Queues            []string
	DisableOutput     bool
}

// retryIntervalsEncode returns a string representation of the retry intervals.
func (o Options) retryIntervalsEncode() string {
	intervals := make([]string, len(o.RetryIntervals))
	for i := range o.RetryIntervals {
		intervals[i] = o.RetryIntervals[i].String()
	}
	return strings.Join(intervals, ", ")
}

func defaultOptions() *Options {
	opts := &Options{}

	options := []Option{
		WithConcurrency(_DEFAULT_CONCURRENCY),
		WithMaxRetries(_DEFAULT_MAX_RETRIES),
		WithTTL(_DEFAULT_TTL),
		WithTimeout(_DEFAULT_TIMEOUT),
		WithRetryIntervals(defaultRetryIntervals),
		WithInitialize(true),
	}

	for i := range options {
		options[i](opts)
	}

	return opts
}

// Option is an option unit.
type Option func(opts *Options)

// WithDisableOutput defines if the output (logo, queues information)
// should be disabled.
func WithDisableOutput(disableOutput bool) Option {
	return func(opts *Options) {
		opts.DisableOutput = disableOutput
	}
}

// WithBroker registers new broker.
func WithBroker(broker Broker) Option {
	return func(opts *Options) {
		opts.Broker = broker
	}
}

// WithQueues allows to override queues to run.
func WithQueues(queues ...string) Option {
	return func(opts *Options) {
		opts.Queues = append(opts.Queues, queues...)
	}
}

// WithQueues allows to override queues to run.
func WithQueuess(queues []string) Option {
	return func(opts *Options) {
		opts.Queues = append(opts.Queues, queues...)
	}
}

// WithSerializer defines the Serializer.
func WithSerializer(serializer Serializer) Option {
	return func(opts *Options) {
		opts.Serializer = serializer
	}
}

// WithInitialize defines if the broker needs to be initialized.
func WithInitialize(initialize bool) Option {
	return func(opts *Options) {
		opts.Initialize = initialize
	}
}

// WithLogger defines the Logger.
func WithLogger(logger *ekalog.Logger) Option {
	return func(opts *Options) {
		opts.Logger = logger
		opts.loggerIsPresented = true
	}
}

// WithTimeout defines the timeout used to execute a task.
func WithTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.Timeout = timeout
	}
}

// WithCountdown defines the countdown to launch a delayed task.
func WithCountdown(countdown time.Duration) Option {
	return func(opts *Options) {
		opts.Countdown = &countdown
	}
}

// WithConcurrency defines the number of concurrent consumers.
func WithConcurrency(concurrency int) Option {
	return func(opts *Options) {
		opts.Concurrency = concurrency
	}
}

// WithMaxRetries defines the number of maximum retries for a failed task.
func WithMaxRetries(maxRetries int) Option {
	return func(opts *Options) {
		opts.MaxRetries = maxRetries
	}
}

// WithRetryIntervals defines the retry intervals for a failed task.
func WithRetryIntervals(retryIntervals []time.Duration) Option {
	return func(opts *Options) {
		opts.RetryIntervals = retryIntervals
	}
}

// WithTTL defines the duration to keep the task in the broker.
func WithTTL(ttl time.Duration) Option {
	return func(opts *Options) {
		opts.TTL = ttl
	}
}
