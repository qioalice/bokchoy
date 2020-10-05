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

type (
	// Options is the bokchoy options.
	options struct {

		// --- Required options ---

		Broker            Broker
		Serializer        Serializer

		// --- Additional options ---

		Logger            *ekalog.Logger
		loggerIsPresented bool

		Concurrency       int8
		MaxRetries        int8
		TTL               time.Duration
		Countdown         time.Duration
		Timeout           time.Duration
		RetryIntervals    []time.Duration
		Initialize        bool
		Queues            []string
		DisableOutput     bool
	}
)

var (
	defaultOptions *options
)

func (o *options) apply(options []Option) {
	for _, option := range options {
		if option != nil {
			option(o)
		}
	}
}

func initDefaultOptions() {
	defaultOptions = new(options)
	defaultOptions.apply([]Option{
		WithConcurrency(_DEFAULT_CONCURRENCY),
		WithMaxRetries(_DEFAULT_MAX_RETRIES),
		WithTTL(_DEFAULT_TTL),
		WithTimeout(_DEFAULT_TIMEOUT),
		WithRetryIntervals(defaultRetryIntervals),
		WithInitialize(true),
	})
}

// retryIntervalsEncode returns a string representation of the retry intervals.
func (o options) retryIntervalsEncode() string {
	intervals := make([]string, len(o.RetryIntervals))
	for i := range o.RetryIntervals {
		intervals[i] = o.RetryIntervals[i].String()
	}
	return strings.Join(intervals, ", ")
}
