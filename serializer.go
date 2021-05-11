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
	"github.com/qioalice/ekago/v3/ekaerr"
)

// Serializer defines an interface to implement a serializer,
// to encode user's Task payload to be a part of encoded RAW data of tasks,
// that will be used by Broker.
type Serializer interface {
	Dumps(interface{}) ([]byte, *ekaerr.Error)
	Loads([]byte, *interface{}) *ekaerr.Error
	IsHumanReadable() bool
	Name() string
}
