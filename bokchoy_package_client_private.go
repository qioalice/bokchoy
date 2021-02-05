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
	"sync"
)

var (
	defaultClient *Bokchoy
)

func createPackageLevelClient() {
	// since we don't initializing wg, defaultClient.isValid() will fail anyway
	// before Init() would be called explicitly, but initialization allows us
	// to use at least sema
	defaultClient = new(Bokchoy)
	defaultClient.sema = &sync.Mutex{}
}
