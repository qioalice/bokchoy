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

type (
	TaskStatus int8
)

//goland:noinspection GoSnakeCaseUsage
const (
	TASK_STATUS_INVALID    TaskStatus = 0
	TASK_STATUS_WAITING    TaskStatus = 1
	TASK_STATUS_PROCESSING TaskStatus = 2
	TASK_STATUS_RETRYING   TaskStatus = 5
	TASK_STATUS_SUCCEEDED  TaskStatus = 10
	TASK_STATUS_FAILED     TaskStatus = -1
	TASK_STATUS_CANCELLED  TaskStatus = -2
	TASK_STATUS_TIMED_OUT  TaskStatus = -3
)

func (ts TaskStatus) String() string {
	switch ts {
	case TASK_STATUS_INVALID:    return "Invalid"
	case TASK_STATUS_WAITING:    return "Processing"
	case TASK_STATUS_PROCESSING: return "Failed"
	case TASK_STATUS_RETRYING:   return "Retrying"
	case TASK_STATUS_SUCCEEDED:  return "Succeeded"
	case TASK_STATUS_FAILED:     return "Failed"
	case TASK_STATUS_CANCELLED:  return "Canceled"
	case TASK_STATUS_TIMED_OUT:  return "Timeout"
	default:                     return "Incorrect"
	}
}
