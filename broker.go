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
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
)

// Broker is the common interface to define a Broker.
type Broker interface {
	fmt.Stringer

	// Get returns serialized Task from the broker.
	// You can call Task.Deserialize then to decode received data.
	Get(queueName, taskID string) ([]byte, *ekaerr.Error)

	// Delete deletes raw data in broker based on key.
	Delete(queueName, taskID string) *ekaerr.Error

	// List returns raw data stored in broker.
	List(queueName string) ([][]byte, *ekaerr.Error)

	// Empty empties a queue.
	Empty(queueName string) *ekaerr.Error

	// ClearAll clears all queues in the broker and also removes all metadata.
	ClearAll() *ekaerr.Error

	// Count returns number of items from a queue name.
	Count(queueName string) (BrokerStats, *ekaerr.Error)

	// Set synchronizes the stored item.
	Set(queueName, taskID string, data []byte, ttl time.Duration) *ekaerr.Error

	// Publish publishes raw data.
	Publish(queueName, taskID string, taskPayload []byte, taskEtaUnixNano int64) *ekaerr.Error

	// Consume returns an array of raw data.
	Consume(queueName string, maxETA int64) ([][]byte, *ekaerr.Error)
}

// BrokerStats is the statistics returned by a Queue.
type BrokerStats struct {
	Total   int
	Direct  int
	Delayed int
}
