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

type (
	serializerDummy struct {}
)

var (
	defaultSerializerDummy Serializer = new(serializerDummy)
)

func DefaultSerializerDummy() Serializer {
	return defaultSerializerDummy
}

func (_ *serializerDummy) Dumps(_ interface{}) ([]byte, *ekaerr.Error) {
	return nil, nil
}

func (_ *serializerDummy) Loads(_ []byte, _ *interface{}) *ekaerr.Error {
	return nil
}

func (_ *serializerDummy) IsHumanReadable() bool {
	return true
}

func (_ *serializerDummy) Name() string {
	return "Internal dummy serializer. Does nothing."
}

var _ Serializer = (*serializerDummy)(nil)
