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
	"github.com/stretchr/testify/require"
)

func TestSerializerJSON(t *testing.T) {

	type T struct { I int }
	var (t1 T; t2, t3 interface{})

	const JSON = `{"i":42}`

	ser := bokchoy.CustomSerializerJSON(T{})

	t1.I = 42
	t1G, err := ser.Dumps(t1)
	err.LogAsFatal()

	ser.Loads([]byte(JSON), &t2).LogAsFatal()
	ser.Loads(t1G, &t3).LogAsFatal()

	require.Equal(t, t1, t2)
	require.Equal(t, t1, t3)
	require.Equal(t, t2, t3)

	spew.Dump(t3)
}
