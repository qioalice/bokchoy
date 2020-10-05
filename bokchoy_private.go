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
	"fmt"
	"os"
	"os/user"

	"github.com/qioalice/ekago/v2/ekasys"
)

func (b *Bokchoy) isValid() bool {
	return b != nil && b.wg != nil && b.sema != nil
}

// queueNames returns the managed queue names.
func (b *Bokchoy) queueNames() []string {
	names := make([]string, 0, len(b.queues))

	for k := range b.queues {
		names = append(names, k)
	}

	return names
}

func (b *Bokchoy) displayOutput(queueNames []string) {

	buf := newColorWriter(colorBrightGreen)
	buf.Write("%s\n", _LOGO)
	buf = buf.WithColor(colorBrightBlue)

	usr, err := user.Current()
	if err == nil {
		hostname, err := os.Hostname()
		if err == nil {
			buf.Write(" %s@%s %v\n", usr.Username, hostname, VERSION)
			buf.Write(" - UID: %s\n", usr.Uid)
			buf.Write(" - GID: %s\n\n", usr.Gid)
		}
	}

	srn := b.serializer.Name()
	srhr := b.serializer.IsHumanReadable()

	buf.Write("	[config]\n")
	buf.Write("	- Broker:          %s\n", b.broker.String())
	buf.Write("	- Serializer:      %s (Human-readable: %t)\n", srn, srhr)
	buf.Write("	- Concurrency:     %d\n", b.defaultOptions.Concurrency)
	buf.Write("	- Max retries:     %d\n", b.defaultOptions.MaxRetries)
	buf.Write("	- Retry intervals: %s\n", b.defaultOptions.retryIntervalsEncode())
	buf.Write("	- TTL:             %s\n", b.defaultOptions.TTL)
	buf.Write("	- Countdown:       %s\n", b.defaultOptions.Countdown)
	buf.Write("	- Timeout:         %s\n", b.defaultOptions.Timeout)

	buf.Write("\n	[queues]\n")

	for i := range queueNames {
		buf.Write(fmt.Sprintf("	- %s\n", queueNames[i]))
	}

	_, _ = ekasys.Stdout().Write(buf.Bytes())
}