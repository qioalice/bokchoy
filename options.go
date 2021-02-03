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
	"time"

	"github.com/qioalice/ekago/v2/ekalog"
)

// Option is an option unit.
type (
	Option func(opts *options)
)

// WithDisableOutput defines if the output (logo, queues information)
// should be disabled.
func WithDisableOutput(disableOutput bool) Option {
	return func(opts *options) {
		opts.DisableOutput = disableOutput
	}
}

// WithBroker registers new broker.
func WithBroker(broker Broker) Option {
	return func(opts *options) {
		opts.Broker = broker
	}
}

// WithQueues allows to override queues to run.
func WithQueues(queues ...string) Option {
	return func(opts *options) {
		opts.Queues = append(opts.Queues, queues...)
	}
}

// WithQueuess allows to override queues to run.
func WithQueuess(queues []string) Option {
	return func(opts *options) {
		opts.Queues = append(opts.Queues, queues...)
	}
}

// WithSerializer defines the Serializer.
func WithSerializer(serializer Serializer) Option {
	return func(opts *options) {
		opts.Serializer = serializer
	}
}

// WithInitialize defines if the broker needs to be initialized.
func WithInitialize(initialize bool) Option {
	return func(opts *options) {
		opts.Initialize = initialize
	}
}

// WithLogger defines the Logger.
func WithLogger(logger *ekalog.Logger) Option {
	return func(opts *options) {
		opts.Logger = logger
		opts.loggerIsPresented = true
	}
}

// WithTimeout defines the timeout used to execute a task.
func WithTimeout(timeout time.Duration) Option {
	if timeout < 0 {
		timeout = 0
	}
	return func(opts *options) {
		opts.Timeout = timeout
	}
}

// WithCountdown defines the countdown to launch a delayed task.
func WithCountdown(countdown time.Duration) Option {
	return func(opts *options) {
		opts.Countdown = countdown
	}
}

// WithConcurrency defines the number of concurrent consumers.
func WithConcurrency(concurrency int8) Option {
	if concurrency <= 0 {
		concurrency = 1
	}
	return func(opts *options) {
		opts.Concurrency = concurrency
	}
}

// WithMaxRetries defines the number of maximum retries for a failed task.
func WithMaxRetries(maxRetries int8) Option {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return func(opts *options) {
		opts.MaxRetries = maxRetries
	}
}

// WithRetryIntervals defines the retry intervals for a failed task.
func WithRetryIntervals(retryIntervals []time.Duration) Option {
	validIntervals := make([]time.Duration, 0, len(retryIntervals))
	for _, retryInterval := range retryIntervals {
		if retryInterval >= 1 * time.Microsecond {
			validIntervals = append(validIntervals, retryInterval)
		}
	}
	if len(validIntervals) == 0 {
		return nil
	}
	return func(opts *options) {
		opts.RetryIntervals = retryIntervals
	}
}

// WithTTL defines the duration to keep the task in the broker.
func WithTTL(ttl time.Duration) Option {
	if ttl < 0 {
		ttl = 0
	}
	return func(opts *options) {
		opts.TTL = ttl
	}
}
