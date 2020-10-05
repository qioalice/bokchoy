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

	"github.com/qioalice/ekago/v2/ekadanger"
	"github.com/qioalice/ekago/v2/ekaerr"

	"github.com/davecgh/go-spew/spew" // deep dumper
	"github.com/json-iterator/go" // fast than encoding/json
	"github.com/modern-go/reflect2" // fast than reflect (w/ caching)
)

type (
	serializerJSON struct {}
)

var (
	defaultSerializerJSON Serializer = new(serializerJSON)
)

func DefaultSerializerJSON() Serializer {
	return defaultSerializerJSON
}

func (_ *serializerJSON) Dumps(v interface{}) ([]byte, *ekaerr.Error) {
	data, legacyErr := jsoniter.Marshal(v)
	if legacyErr != nil {

		vType := "<nil>"
		if v != nil {
			vType = reflect2.TypeOf(v).String()
		}

		return nil, ekaerr.InternalError.
			Wrap(legacyErr, "Bokchoy.serializerJSON: Failed to serialize").
			AddFields(
				"bokchoy_serializer_obj_data", spew.Sdump(v),
				"bokchoy_serializer_obj_type", vType).
			Throw()
	}

	return data, nil
}

func (_ *serializerJSON) Loads(data []byte, v interface{}) *ekaerr.Error {
	legacyErr := jsoniter.Unmarshal(data, v)
	if legacyErr != nil {

		vAddr := ekadanger.TakeRealAddr(v)
		vType := "<nil>"
		if v != nil {
			vType = reflect2.TypeOf(v).String()
		}

		return ekaerr.InternalError.
			Wrap(legacyErr, "Bokchoy.serializerJSON: Failed to deserialize").
			AddFields(
				"bokchoy_serializer_raw_data_as_hex", hex.EncodeToString(data),
				"bokchoy_serializer_destination_type", vType,
				"bokchoy_serializer_destination_value", vAddr).
			Throw()
	}

	return nil
}

func (_ *serializerJSON) IsHumanReadable() bool {
	return true
}

func (_ *serializerJSON) Name() string {
	return "JSON, based on: https://github.com/json-iterator/go"
}
