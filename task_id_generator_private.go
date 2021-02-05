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
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

type (
	// taskIdGen is a helper struct that provides thread-safety math/rand based
	// ULID generation using newId() method. Requires init() be called once per instance.
	taskIdGen struct {
		mu   sync.Mutex
		rand io.Reader
	}
)

// init creates a new math/rand RNG reader for generating ULID.
//
// Yes, i avoid to use crypto/rand, because it's 10x slower than math/rand,
// according with benchmarks from https://github.com/oklog/ulid#benchmarks .
func (g *taskIdGen) init() {
	g.rand = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
}

// newId generates and returns a new ULID. Thread-safety.
func (g *taskIdGen) newId() string {
	// defer is not free
	g.mu.Lock()
	ret := ulid.MustNew(ulid.Now(), g.rand).String()
	g.mu.Unlock()
	return ret
}
