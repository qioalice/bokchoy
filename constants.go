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
)

//goland:noinspection GoSnakeCaseUsage
const (
	_LOGO = `
	 _           _        _                 
	| |__   ___ | | _____| |__   ___  _   _ 
	| '_ \ / _ \| |/ / __| '_ \ / _ \| | | |
	| |_) | (_) |   < (__| | | | (_) | |_| |
	|_.__/ \___/|_|\_\___|_| |_|\___/ \__, |
	                                  |___/ 
`
	_DEFAULT_TIMEOUT     = 180 * time.Second
	_DEFAULT_CONCURRENCY = 1
	_DEFAULT_MAX_RETRIES = 3
	_DEFAULT_TTL         = 180 * time.Second

	VERSION = "v1.0.6, 08 October 2020, 13:42 GMT+3"
)

var defaultRetryIntervals = []time.Duration{
	60 * time.Second,
	120 * time.Second,
	180 * time.Second,
}
