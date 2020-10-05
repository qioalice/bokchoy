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
	"math/rand"
	"strings"
	"time"

	"github.com/oklog/ulid"
)

func ID() string {
	t := time.Now().UTC()
	entropy := rand.New(rand.NewSource(t.UnixNano()))
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}

func BuildKey(parts ...string) string {

	var b strings.Builder

	if len(parts) == 0 || parts[0] == "" {
		return ""
	}

	_, _ = b.WriteString(parts[0])
	for i, n := 1, len(parts); i < n && parts[i] != ""; i++ {
		_ = b.WriteByte('/')
		_, _ = b.WriteString(parts[i])
	}

	return b.String()
}
