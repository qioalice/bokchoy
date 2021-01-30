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

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekaunsafe"

	"github.com/davecgh/go-spew/spew" // deep dumper
	"github.com/json-iterator/go"     // fast than encoding/json
	"github.com/modern-go/reflect2"   // fast than reflect (w/ caching)
)

type (
	serializerJSON struct {
		typIsPresented bool
		typ reflect2.Type
	}
)

var (
	defaultSerializerJSON Serializer = new(serializerJSON)
)

func DefaultSerializerJSON() Serializer {
	return defaultSerializerJSON
}

func CustomSerializerJSON(typ reflect2.Type) Serializer {
	return &serializerJSON{
		typIsPresented: true,
		typ:            typ,
	}
}

func (q *serializerJSON) Dumps(v interface{}) ([]byte, *ekaerr.Error) {
	const s = "Bokchoy.SerializerJSON: Failed to serialize. "

	if q.typIsPresented {
		t := reflect2.TypeOf(v)
		if t.RType() != q.typ.RType() {
			return nil, ekaerr.IllegalArgument.
				New(s + "Unexpected data type. Must be the same.").
				AddFields(
					"bokchoy_serializer_want_rtype", q.typ.RType(),
					"bokchoy_serializer_want_type", q.typ.String(),
					"bokchoy_serializer_got_rtype", t.RType(),
					"bokchoy_serializer_got_type", t.String()).
				Throw()
		}
	}

	data, legacyErr := jsoniter.Marshal(v)
	if legacyErr != nil {

		vType := "<nil>"
		if v != nil {
			vType = reflect2.TypeOf(v).String()
		}

		return nil, ekaerr.InternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_serializer_obj_data", spew.Sdump(v),
				"bokchoy_serializer_obj_type", vType).
			Throw()
	}

	return data, nil
}

func (q *serializerJSON) Loads(data []byte, v *interface{}) *ekaerr.Error {
	const s = "Bokchoy.SerializerJSON: Failed to deserialize. "

	var (
		injectDest interface{}
		legacyErr error
	)

	if q.typIsPresented {
		if v == nil {
			return ekaerr.IllegalArgument.
				New(s + "Nil pointer destination.").
				AddFields(
					"bokchoy_serializer_want_rtype", q.typ.RType(),
					"bokchoy_serializer_want_type", q.typ.String()).
				Throw()
		}

		injectDest = q.typ.New()
		legacyErr = jsoniter.Unmarshal(data, injectDest)

	} else {
		if v == nil {
			return ekaerr.IllegalArgument.
				New(s + "Nil pointer destination.").
				Throw()
		}

		legacyErr = jsoniter.Unmarshal(data, v)
	}

	if legacyErr != nil {
		vAddr := ekaunsafe.TakeRealAddr(*v)
		vType := "<nil>"
		if *v != nil {
			vType = reflect2.TypeOf(v).String()
		}

		return ekaerr.InternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_serializer_raw_data_as_hex", hex.EncodeToString(data),
				"bokchoy_serializer_destination_type", vType,
				"bokchoy_serializer_destination_value", vAddr).
			Throw()
	}

	if q.typIsPresented {
		*v = q.typ.Indirect(injectDest)
	}

	return nil

}

func (_ *serializerJSON) IsHumanReadable() bool {
	return true
}

func (_ *serializerJSON) Name() string {
	return "JSON, based on: https://github.com/json-iterator/go"
}
