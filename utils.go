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

func TaskKey(queueName, taskID string) string {
	return __buildRedisKeyTriple(queueName, taskID, "")
}

func __buildRedisKeyTriple(part1, part2, part3 string) string {

	var b strings.Builder

	totalLen := 8 // len of "bokchoy/"
	totalLen += len(part1)
	if part2 != "" {
		totalLen += len(part2) +1
	}
	if part3 != "" {
		totalLen += len(part3) +1
	}

	b.Grow(totalLen)

	if part1 != "" {
		_, _ = b.WriteString(part1)
	}
	if part2 != "" {
		if b.Len() > 0 {
			_ = b.WriteByte('/')
		}
		_, _ = b.WriteString(part2)
	}
	if part3 != "" {
		if b.Len() > 0 {
			_ = b.WriteByte('/')
		}
		_, _ = b.WriteString(part3)
	}

	return b.String()
}
