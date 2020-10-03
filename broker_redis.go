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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/qioalice/ekago/v2/ekadanger"
	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekalog"
	"github.com/qioalice/ekago/v2/ekatime"

	"github.com/go-redis/redis/v7"
)

type (
	// RedisBroker is the redis broker.
	RedisBroker struct {
		client     redis.UniversalClient
		clientInfo string
		logger     *ekalog.Logger
		scripts    map[string]string
		mu         *sync.Mutex
		qcd        map[string]struct{} // queues must be consumed delayed
	}
)

var _ Broker = (*RedisBroker)(nil)

// NewRedisBroker initializes a new redis broker instance.
func NewRedisBroker(clt redis.UniversalClient, logger *ekalog.Logger) *RedisBroker {

	clientInfo := "<Incorrect Redis client>"
	if clt != nil && ekadanger.TakeRealAddr(clt) != nil {

		// Go to options and extract address.
		// C-style magic.
		// Let's gonna be dirty!
		//
		// So, each Redis' client contains its private part by pointer,
		// which contains its options by pointer, like:
		//
		//    type SomeRedisClient struct {
		//        *someRedisClientPrivate
		//        ...
		//    }
		//    type someRedisClientPrivate struct {
		//        *someRedisClientPrivateOptions
		//        ...
		//    }
		//    type someRedisClientPrivateOptions struct {
		//        < that's what we need >
		//    }
		//
		// So, it's not hard.
		fn := func(ptr unsafe.Pointer, typ int) string {
			// ptr is either *redis.Client, *redis.ClusterClient or *redis.Ring
			// we have to dereference it, and then we'll receive a pointer
			// to the either *redis.baseClient, *redis.clusterClient or *redis.ring
			doublePtr := (*unsafe.Pointer)(ptr)
			ptr = *doublePtr
			// once more to get options
			doublePtr = (*unsafe.Pointer)(ptr)
			ptr = *doublePtr
			// and that's it

			switch typ {
			case 1:
				options := (*redis.Options)(ptr)
				return fmt.Sprintf("Redis client, [%s], %d DB",
					options.Addr, options.DB)

			case 2:
				options := (*redis.ClusterOptions)(ptr)
				return fmt.Sprintf("Redis cluster, [%s]",
					strings.Join(options.Addrs, ", "))

			case 3:
				options := (*redis.RingOptions)(ptr)
				addresses := make([]string, 0, len(options.Addrs))
				for _, addr := range options.Addrs {
					addresses = append(addresses, addr)
				}
				return fmt.Sprintf("Redis ring, [%s], %d DB",
					strings.Join(addresses, ", "), options.DB)

			default:
				return "<Unknown Redis client>"
			}
		}

		switch client := clt.(type) {
		case *redis.Client:        clientInfo = fn(unsafe.Pointer(client), 1)
		case *redis.ClusterClient: clientInfo = fn(unsafe.Pointer(client), 2)
		case *redis.Ring:          clientInfo = fn(unsafe.Pointer(client), 3)
		}
	}

	return &RedisBroker{
		client:     clt,
		clientInfo: clientInfo,
		logger:     logger,
		qcd:        make(map[string]struct{}),
		mu:         &sync.Mutex{},
	}
}

func (p *RedisBroker) String() string {
	if p == nil {
		return "<Incorrect Redis client>"
	}
	return p.clientInfo
}

// Initialize initializes the redis broker.
func (p *RedisBroker) Initialize(_ context.Context) *ekaerr.Error {

	if err := p.Ping(); err.IsNotNil() {
		return err.
			Throw()
	}

	p.scripts = make(map[string]string)
	for scriptName, scriptData := range redisScripts {
		sha, legacyErr := p.client.ScriptLoad(scriptData).Result()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to load additional Redis script").
				AddFields(
					"bokchoy_init_script_name", scriptName,
					"bokchoy_broker", p.clientInfo).
				Throw()
		}

		p.scripts[scriptName] = sha
	}

	return nil
}

// Ping pings the redis broker to ensure it's well connected.
func (p RedisBroker) Ping() *ekaerr.Error {

	if legacyErr := p.client.Ping().Err(); legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to ping Redis server").
			AddFields("bokchoy_broker", p.clientInfo).
			Throw()
	}

	return nil
}

// Consume returns an array of raw data.
func (p *RedisBroker) Consume(ctx context.Context, name string, eta ekatime.Timestamp) ([]map[string]interface{}, *ekaerr.Error) {
	return p.consume(ctx, name, name, eta)
}

func (p *RedisBroker) payloadsFromKeys(taskKeys []string) (map[string]map[string]interface{}, *ekaerr.Error) {

	vals, legacyErr := p.client.EvalSha(p.scripts["MULTIHGETALL"], taskKeys).Result()
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to receive payloads by its keys").
			AddFields(
				"bokchoy_task_keys", strings.Join(taskKeys, ", "),
				"bokchoy_error_redis_command", "MULTIHGETALL").
			Throw()
	}

	var values map[string]map[string]interface{}
	legacyErr = json.Unmarshal([]byte(vals.(string)), &values)
	if legacyErr != nil {
		return nil, ekaerr.InternalError.
			Wrap(legacyErr, "Bokchoy: Failed to receive payloads by its keys. " +
				"Unable to decode").
			AddFields(
				"bokchoy_task_keys", strings.Join(taskKeys, ", "),
				"bokchoy_raw_data", vals.(string),
				"bokchoy_raw_data_as_hex", hex.EncodeToString([]byte(vals.(string)))).
			Throw()
	}

	return values, nil
}

// Get returns stored raw data from task key.
func (p *RedisBroker) Get(taskKey string) (map[string]interface{}, *ekaerr.Error) {
	taskKey = p.buildKey(taskKey, "")

	res, legacyErr := p.client.HGetAll(taskKey).Result()
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get task").
			AddFields(
				"bokchoy_task_key", taskKey,
				"bokchoy_error_redis_command", "HGETALL").
			Throw()
	}

	results := make(map[string]interface{})
	for k, v := range res {
		results[k] = v
	}

	return results, nil
}

// Delete deletes raw data in broker based on key.
func (p *RedisBroker) Delete(queueName, taskID string) *ekaerr.Error {
	prefixedTaskKey := p.buildKey(queueName, taskID)

	_, legacyErr := p.client.Del(prefixedTaskKey).Result()
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to delete task").
			AddFields(
				"bokchoy_task_key", prefixedTaskKey,
				"bokchoy_task_id", taskID,
				"bokchoy_error_redis_command", "DEL").
			Throw()
	}

	return nil
}

func (p *RedisBroker) List(queueName string) ([]map[string]interface{}, *ekaerr.Error) {
	taskIDs, legacyErr := p.client.LRange(queueName, 0, -1).Result()
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get tasks").
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "LRANGE").
			Throw()
	}

	taskKeys := make([]string, 0, len(taskIDs))
	for i, n := 0, len(taskIDs); i < n; i++ {
		taskKeys = append(taskKeys, p.buildKey(queueName, taskIDs[i]))
	}

	payloads, err := p.payloadsFromKeys(taskKeys)
	if err.IsNotNil() {
		return nil, err.
			AddMessage("Bokchoy: Failed to get tasks").
			AddFields("bokchoy_queue_name", queueName).
			Throw()
	}

	results := make([]map[string]interface{}, 0, len(taskKeys))
	for _, data := range payloads {
		if len(data) == 0 {
			continue
		}

		results = append(results, data)
	}

	return results, nil
}

// Count returns number of items from a queue name.
func (p *RedisBroker) Count(queueName string) (BrokerStats, *ekaerr.Error) {

	var stats BrokerStats
	queueName = p.buildKey(queueName, "")

	direct, legacyErr := p.client.LLen(queueName).Result()
	if legacyErr != nil && legacyErr != redis.Nil {
		return stats, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get a queue stat").
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "LLEN").
			Throw()
	}

	delayed, legacyErr := p.client.ZCount(queueName + ":delay", "-inf", "+inf").Result()
	if legacyErr != nil && legacyErr != redis.Nil {
		return stats, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get a queue stat").
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "ZCOUNT").
			Throw()
	}

	stats.Direct = int(direct)
	stats.Delayed = int(delayed)
	stats.Total = stats.Direct + stats.Delayed

	return stats, nil
}

// Save synchronizes the stored item in redis.
func (p *RedisBroker) Set(

	taskKey string,
	data map[string]interface{},
	expiration time.Duration,

) *ekaerr.Error {

	taskKey = p.buildKey(taskKey, "")

	if int(expiration.Seconds()) == 0 {
		_, legacyErr := p.client.HMSet(taskKey, data).Result()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to sync task").
				AddFields(
					"bokchoy_task_key", taskKey,
					"bokchoy_error_redis_command", "HMSET").
				Throw()
		}

		return nil
	}

	values := []interface{}{int(expiration.Seconds())}
	values = append(values, unpack(data)...)

	_, legacyErr := p.client.EvalSha(
		p.scripts["HMSETEXPIRE"], []string{taskKey}, values...,
	).Result()
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to sync task").
			AddFields(
				"bokchoy_task_key", taskKey,
				"bokchoy_error_redis_command", "HMSETEXPIRE").
			Throw()
	}

	return nil
}

// Publish publishes raw data.
// it uses a hash to store the task itself
// pushes the task id to the list or a zset if the task is delayed.
func (p *RedisBroker) Publish(

	queueName,
	taskID     string,
	data       map[string]interface{},
	eta        ekatime.Timestamp,

) *ekaerr.Error {

	_, legacyErr := p.client.Pipelined(func(pipe redis.Pipeliner) error {
		err := p.publish(pipe, queueName, taskID, data, eta)
		return wrapEkaerr(err.Throw())
	})

	var err *ekaerr.Error

	if err = extractEkaerr(legacyErr); err.IsNil() && legacyErr != nil {
		err = ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to publish task")
	}

	//goland:noinspection GoNilness
	if err.IsNotNil() {
		return err.
			AddFields(
				"bokchoy_task_id", taskID,
				"bokchoy_queue_name", queueName).
			Throw()
	}

	return nil
}

// Empty removes the redis key for a queue.
func (p *RedisBroker) Empty(queueName string) *ekaerr.Error {

	queueKey := p.buildKey(queueName, "")

	legacyErr := p.client.Del(queueName).Err()
	if legacyErr != nil && legacyErr != redis.Nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to empty queue").
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_queue_key", queueKey).
			Throw()
	}

	return nil
}

// ClearAll removes the whole Redis database.
//
// WARNING! ABSOLUTELY ALL DATA WILL BE WIPED!
// NOT ONLY BOKCHOY'S BUT ABSOLUTELY ALL!
func (p *RedisBroker) ClearAll() *ekaerr.Error {

	// TODO: Replace FLUSHDB -> DEL of KEYS (remove only Bokchoy's data)

	if legacyErr := p.client.FlushDB().Err(); legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to wipe whole database").
			Throw()
	}

	return nil
}
