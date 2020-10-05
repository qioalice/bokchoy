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
	"encoding/hex"
	"strings"
	"time"

	"github.com/qioalice/ekago/v2/ekadanger"
	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekatime"

	"github.com/davecgh/go-spew/spew"
)

type (
	// Task is the model stored in a Queue.
	Task struct {
		id             string
		queueName      string
		PublishedAt    ekatime.Timestamp
		startedAt      int64 // unix nano
		processedAt    int64 // unix nano
		status         TaskStatus
		oldStatus      TaskStatus
		MaxRetries     int8
		Error          *ekaerr.Error
		Panic          interface{}
		Payload        interface{}
		payloadEncoded []byte
		payloadOldAddr uintptr
		ExecTime       time.Duration
		TTL            time.Duration
		Timeout        time.Duration
		ETA            ekatime.Timestamp
		RetryIntervals []time.Duration
	}
)

func (t *Task) ID() string {

	if !t.isValid() {
		return ""
	}

	return t.id
}

//
func (t *Task) QueueName() string {

	if !t.isValid() {
		return ""
	}

	return t.queueName
}


// Key returns a Task's key that must be used
// implementing read/write access to the Task in the Bokchoy backend.
//
// Generally, returns a string "t.QueueName/t.ID".
//
// Requirements:
// - Task is valid. Otherwise empty string is returned.
func (t *Task) Key() string {

	if !t.isValid() {
		return ""
	}

	var sb strings.Builder
	sb.Grow(1 + len(t.queueName) + len(t.id))

	_, _ = sb.WriteString(t.queueName)
	_    = sb.WriteByte('/')
	_, _ = sb.WriteString(t.id)

	return sb.String()
}

// Status returns the Task's status, that:
//  - Has been sent by you, or
//  - Task had at the moment when you retrieve the Task from a Bokchoy backend.
//
// Requirements:
//  - Current Task is valid. Otherwise TASK_STATUS_INVALID is returned.
//
// WARNING!
// TAKE A LOOK ALSO AT THE IsFinished() METHOD.
func (t *Task) Status() TaskStatus {

	if !t.isValid() {
		return TASK_STATUS_INVALID
	}

	return t.status
}

func (t *Task) MarkAsSucceeded() {
	t.processedAt = time.Now().UTC().UnixNano()
	t.status = TASK_STATUS_SUCCEEDED
	t.ExecTime = time.Duration(t.processedAt - t.startedAt)
}

func (t *Task) MarkAsCanceled() {
	t.processedAt = time.Now().UTC().UnixNano()
	t.status = TASK_STATUS_CANCELLED
}

// Finished returns if a task is finished or not.
func (t *Task) IsFinished() bool {

	if !t.isValid() {
		return false
	}

	return (t.oldStatus == TASK_STATUS_SUCCEEDED) ||
		((t.oldStatus == TASK_STATUS_FAILED || t.status == TASK_STATUS_FAILED) &&
			(t.MaxRetries == 0)) ||
		(t.status == TASK_STATUS_SUCCEEDED)
}

// Serialize serializes a Task to raw data.
func (t *Task) Serialize(userPayloadSerializer Serializer) ([]byte, *ekaerr.Error) {
	const s = "Bokchoy: Failed to encode task using msgpack. "
	switch {

	case !t.isValid():
		return nil, ekaerr.IllegalArgument.
			New(s + "Task is invalid. Has it been initialized correctly?").
			AddFields("bokchoy_task_why_invalid", t.whyInvalid()).
			Throw()

	case userPayloadSerializer == nil:
		return nil, ekaerr.IllegalArgument.
			New(s + "User payload serializer is nil.").
			Throw()
	}

	needToEncodePayload := true

	// Maybe payload has not been changed?
	payloadDataAddr := ekadanger.TakeRealAddr(t.Payload)
	if uintptr(payloadDataAddr) == t.payloadOldAddr && len(t.payloadEncoded) > 0 {
		needToEncodePayload = false
	}

	if needToEncodePayload {
		encodedPayload, err := userPayloadSerializer.Dumps(t.Payload)
		if err.IsNotNil() {
			return nil, err.
				AddMessage(s + "Failed to serialize user payload.").
				AddFields(
					"bokchoy_task_id", t.ID,
					"bokchoy_task_user_payload", spew.Sdump(t.Payload)).
				Throw()
		}
		t.payloadEncoded = encodedPayload

	}

	output, legacyErr := t.toMsgpackView().MarshalMsg(nil)
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s + "Failed to encode task object.").
			AddFields(
				"bokchoy_task_id", t.ID,
				"bokchoy_user_payload", spew.Sdump(t.Payload)).
			Throw()
	}

	return output, nil
}

// TaskFromPayload returns a Task instance from raw data.
func (t *Task) Deserialize(data []byte, userPayloadSerializer Serializer) *ekaerr.Error {
	const s = "Bokchoy: Failed to decode task using msgpack. "
	switch {

	case t == nil:
		return ekaerr.IllegalArgument.
			New(s + "Task destination is nil.").
			Throw()

	case len(data) == 0:
		return ekaerr.IllegalArgument.
			New(s + "Task encoded data is empty.")

	case userPayloadSerializer == nil:
		return ekaerr.IllegalArgument.
			New(s + "User payload serializer is nil.").
			Throw()
	}
	_, legacyErr := t.toMsgpackView().UnmarshalMsg(data)
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, s + "Failed to decode task object.").
			AddFields("bokchoy_task_encoded", string(data)).
			Throw()
	}

	err := userPayloadSerializer.Loads(t.payloadEncoded, &t.Payload)
	if err.IsNotNil() {
		return err.
			AddMessage(s + "Failed to deserialize user payload.").
			AddFields(
				"bokchoy_task_id", t.ID,
				"bokchoy_task_user_payload_encoded", string(t.payloadEncoded),
				"bokchoy_task_user_payload_encoded_as_hex", hex.EncodeToString(t.payloadEncoded)).
			Throw()
	}

	t.payloadOldAddr = uintptr(ekadanger.TakeRealAddr(t.Payload))
	return nil
}
