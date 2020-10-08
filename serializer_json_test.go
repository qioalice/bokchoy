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

package bokchoy_test

import (
	"testing"

	"github.com/qioalice/bokchoy"

	"github.com/davecgh/go-spew/spew"
	"github.com/modern-go/reflect2"
	"github.com/stretchr/testify/require"
)

func TestSerializerJSON_Loads(t *testing.T) {
	type T struct { I int }
	var dest interface{}
	const JSON = `{"i":42}`
	serializer := bokchoy.CustomSerializerJSON(reflect2.TypeOf(new(T)))
	err := serializer.Loads([]byte(JSON), &dest)
	err.LogAsFatal("Fatal to decode.")
	require.True(t, err.IsNil())
	spew.Dump(dest)
}
