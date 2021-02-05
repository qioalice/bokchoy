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
	"unsafe"

	"github.com/qioalice/ekago/v2/ekaerr"
)

type (
	// Task is the model stored in a Queue.
	// taskMsgpackView is a special type that MUST HAVE:
	//
	// - the same fields (and their order) as Task type have
	// - the same fields RAM consumption as Task type have
	//
	// Then, we can just switch type of underlying data with zero cost
	// from Task type to taskMsgpackView just by unsafe pointer casting.
	//
	// It's all for:
	// Msgpack generator https://github.com/tinylib/msgp can not follow explicit
	// declarations types, like time.Duration, ekatime.Timestamp, etc.
	// So, we have to do that small trick.
	taskMsgpackView struct {
		error          *ekaerr.Error `           msg:"-"`
		panic          interface{}   `           msg:"-"`

		PublishedAt    int64         `msg:"pl"` // real type: ekatime.Timestamp

		TTL            int64         `msg:"tl"` // real type: time.Duration
		ETA            int64         `msg:"et"` // real type: ekatime.Timestamp

		RetryIntervals []int64       `msg:"ri"` // real type: []time.Duration
		MaxRetries     int8          `msg:"re"`

		ExecTime       int64         `msg:"ex"`
		Timeout        int64         `msg:"to"` // real type: time.Duration

		payload        interface{}   `           msg:"-"`

		ID             string        `msg:"id"`
		queueName      string        `           msg:"-"`

		StartedAt      int64         `msg:"st"`
		ProcessedAt    int64         `msg:"pr"`

		Status         int8          `msg:"s"`

		PayloadEncoded []byte        `msg:"p"`
		payloadOldAddr uintptr       `           msg:"-"`
	}
)

func (t *Task) toMsgpackView() *taskMsgpackView {
	return (*taskMsgpackView)(unsafe.Pointer(t))
}

func (z *taskMsgpackView) toTask() *Task {
	return (*Task)(unsafe.Pointer(z))
}
