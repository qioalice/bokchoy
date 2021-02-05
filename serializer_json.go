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

	"github.com/davecgh/go-spew/spew" // deep dumper
	"github.com/json-iterator/go"     // fast than encoding/json
	"github.com/modern-go/reflect2"   // fast than reflect (w/ caching)
)

// DefaultSerializerJSON is the same as CustomSerializerJSON(nil).
func DefaultSerializerJSON() Serializer {
	return serializerJSON{}
}

// CustomSerializerJSON returns a new JSON Serializer,
// that expects the same type's values will passed to Serializer.Dumps(),
// Serializer.Loads() as type of value you pass to this constructor.
//
// Using that constructor builds a special JSON Serializer exactly for you,
// meaning that even defining destination for Serializer.Loads() as interface{},
// the underlying type will be always T, that you pass to this constructor.
// Look:
//         var (
//                 ser = CustomSerializerJSON(T{})
//                 dest interface{}
//         )
//         if err := ser.Loads(<...>, &dest); err.IsNotNil() {
//                 _, ok := dest.(T); // ok == true, if no err.
//         }
//
// Passing nil interface{} returns the same Serializer
// as you may get using DefaultSerializerJSON().
//
// It's OK to use both of T or *T as type. What you pass is what you get.
// Value is not important, so you can just use T{} or (*T)(nil).
//
func CustomSerializerJSON(example interface{}) Serializer {
	z := serializerJSON{
		typ: reflect2.TypeOf(example),
	}
	if typ_, ok := example.(reflect2.Type); ok {
		z.typ = typ_
	}
	return z
}

// Dumps calls jsoniter.Marshal() for passed v,
// doing type check if desired type is specified by constructor.
//
// It means, that if you constructed current serializer using CustomSerializerJSON(T{}),
// calling Dumps() for *T or any other type will lead to instant error.
func (z serializerJSON) Dumps(v interface{}) ([]byte, *ekaerr.Error) {
	const s = "Bokchoy.SerializerJSON: Failed to serialize. "

	if z.typ != nil {

		// Serializer was built using CustomSerializerJSON() constructor,
		// not DefaultSerializerJSON(). Check types.

		if t := reflect2.TypeOf(v); t.RType() != z.typ.RType() {
			return nil, ekaerr.IllegalArgument.
				New(s + "Unexpected data type. Must be the same as used in constructor.").
				AddFields(
					"bokchoy_serializer_want_rtype", z.typ.RType(),
					"bokchoy_serializer_want_type",  z.typ.String(),
					"bokchoy_serializer_got_rtype",  t.RType(),
					"bokchoy_serializer_got_type",   t.String()).
				Throw()
		}
	}

	data, legacyErr := jsoniter.Marshal(v)
	if legacyErr != nil {

		// If you think, that v == nil might be filtered by the check above,
		// you're right, but only if rtypeWant != 0.
		// If caller used DefaultSerializerJSON(), rtypeWant == 0
		// and there was no check above.

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

// Loads calls jsoniter.Unmarshal() for passed data and destination,
// doing type check if desired type is specified by constructor.
//
// Type checking is the reason why do you may prefer use CustomSerializerJSON()
// constructor over default one.
// Shortly, after successful Loads() call, v (not &v) will have the same type,
// that you've passed to CustomSerializerJSON() constructor.
// So, if you passed T, it will be T. If *T -> *T.
// Assuming that it's safe to do conversions from interface{}, w/o type checks,
// w/o bad type error handling, etc.
func (z serializerJSON) Loads(data []byte, v *interface{}) *ekaerr.Error {
	const s = "Bokchoy.SerializerJSON: Failed to deserialize. "

	var (
		injectDest interface{} // type is *T, no matter user want T{} or *T
		legacyErr error
	)

	if z.typ != nil {
		if v == nil {
			return ekaerr.IllegalArgument.
				New(s + "Nil pointer destination.").
				AddFields(
					"bokchoy_serializer_want_rtype", z.typ.RType(),
					"bokchoy_serializer_want_type",  z.typ.String()).
				Throw()
		}
		injectDest = z.typ.New()
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
		vType := "<nil>"
		if *v != nil {
			vType = reflect2.TypeOf(*v).String()
		}

		return ekaerr.InternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_serializer_raw_data_as_hex",  hex.EncodeToString(data),
				"bokchoy_serializer_destination_type", vType).
			Throw()
	}

	if z.typ != nil {
		*v = z.typ.Indirect(injectDest)
	}

	return nil
}

// IsHumanReadable always returns true.
// Dumps() generates minified JSON, but it's still kinda human readable,
// unlike msgpack for example.
func (z serializerJSON) IsHumanReadable() bool {
	return true
}

// Name returns string "JSON, based on: https://github.com/json-iterator/go" .
func (z serializerJSON) Name() string {
	return "JSON, based on: https://github.com/json-iterator/go"
}
