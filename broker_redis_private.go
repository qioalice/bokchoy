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
	"strconv"
	"strings"
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekatime"

	"github.com/go-redis/redis/v7"
)

//goland:noinspection GoSnakeCaseUsage
const (
	_REDIS_SCRIPT_HMSETEXPIRE = `
local key = KEYS[1]
local data = ARGV
local ttl = table.remove(data, 1)
local res = redis.call('HMSET', key, unpack(data))
redis.call('EXPIRE', key, ttl)
return res
`
	_REDIS_SCRIPT_ZPOPBYSCORE = `
local key = ARGV[1]
local min = ARGV[2]
local max = ARGV[3]
local results = redis.call('ZRANGEBYSCORE', key, min, max)
local length = #results
if length > 0 then
    redis.call('ZREMRANGEBYSCORE', key, min, max)
    return results
else
    return nil
end
`
	_REDIS_SCRIPT_MULTIHGETALL = `
local collate = function (key)
  local raw_data = redis.call('HGETALL', key)
  local data = {}

  for idx = 1, #raw_data, 2 do
    data[raw_data[idx]] = raw_data[idx + 1]
  end

  return data;
end

local data = {}

for _, key in ipairs(KEYS) do
  data[key] = collate(key)
end

return cjson.encode(data)
`
)

var (
	redisScripts = map[string]string{
		"HMSETEXPIRE":  _REDIS_SCRIPT_HMSETEXPIRE,
		"ZPOPBYSCORE":  _REDIS_SCRIPT_ZPOPBYSCORE,
		"MULTIHGETALL": _REDIS_SCRIPT_MULTIHGETALL,
	}
)

// buildKey builds key for Redis values, like:
// "bokchoy/<part1>/<part2>" if both of 'part1', 'part2' are presented and not empty,
// or "bokchoy/<part1>", "bokchoy//<part2>" if only one of 'part1', 'part2' is presented.
func (p RedisBroker) buildKey(part1, part2 string) string {

	var b strings.Builder

	totalLen := 8 // len of "bokchoy/"
	totalLen += len(part1)
	if part2 != "" {
		totalLen += len(part2) +1
	}

	b.Grow(totalLen)

	_, _ = b.WriteString("bokchoy/")
	b.WriteString(part1)
	if part2 != "" {
		b.WriteByte('/')
		b.WriteString(part2)
	}

	return b.String()
}

func (p *RedisBroker) consumeDelayed(

	ctx          context.Context,
	queueName    string,
	tickInterval time.Duration, // consume all tasks once per interval
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	consumeDelayedWorkerEach := func(
		ctx               context.Context,
		p                 *RedisBroker,
		delayedQueueName,
		originalQueueName string,
		tickInterval      time.Duration,
	) {
		ticker := time.NewTicker(tickInterval)
		for range ticker.C {
			continue_ := p.consumeDelayedWorker(ctx, originalQueueName, delayedQueueName)
			if !continue_ {
				return
			}
		}
	}

	delayedQueueName := queueName + ":delay"
	_, consumeWorkerAlreadyRunning := p.qcd[delayedQueueName]

	if !consumeWorkerAlreadyRunning {
		go consumeDelayedWorkerEach(ctx, p, delayedQueueName, queueName, tickInterval)
		p.qcd[delayedQueueName] = struct{}{}
	}
}

func (p *RedisBroker) consumeDelayedWorker(

	ctx               context.Context,
	originalQueueName,
	delayedQueueName  string,
) (
	continue_ bool,
) {

	maxEta := ekatime.Now()

	results, err := p.consume(ctx, delayedQueueName, originalQueueName, maxEta)
	if err.IsNotNil() && p.logger.IsValid() {
		err.LogAsErrorwUsing(p.logger,
			"Bokchoy: Failed to retrieve delayed payloads (consume)")
	}

	if len(results) == 0 {
		return true
	}

	_, legacyErr := p.client.TxPipelined(func(pipe redis.Pipeliner) error {
		for i, result := range results {
			taskID, ok := result["id"].(string)
			if !ok {
				continue
			}

			err := p.publish(pipe, originalQueueName, taskID, result, 0)
			if err != nil {
				err.
					AddMessage("Bokchoy: Failed to republish delayed tasks.").
					AddFields(
						"bokchoy_republished_before_error", i,
						"bokchoy_republished_to_be", len(results)).
					Throw()
				return wrapEkaerr(err)
			}
		}

		// To avoid data loss, we only remove the range when results are processed
		legacyErr := pipe.ZRemRangeByScore(
			delayedQueueName,
			"0",
			strconv.FormatInt(maxEta.I64(), 10),
		).Err()
		if legacyErr != nil {
			return legacyErr
		}

		return nil
	})

	if legacyErr != nil {
		if err = extractEkaerr(legacyErr); err.IsNil() {
			// Not *ekaerr.Error, then it's:
			// Redis client's Exec() func error (called in TxPipelined())
			// or pipe.ZRemRangeByScore()'s one.
			err = ekaerr.Interrupted.
				Wrap(legacyErr, "Bokchoy: Failed to republish delayed tasks.")
		}
	}

	//goland:noinspection GoNilness, cause IsNotNil() call is nil safe.
	if err.IsNotNil() && p.logger.IsValid() {
		err.
			AddFields(
				"bokchoy_queue_name", originalQueueName,
				"bokchoy_queue_name_delayed", delayedQueueName).
			LogAsErrorwwUsing(p.logger,
				"Failed to consume delayed tasks.", nil)
	}

	return true
}

func (p *RedisBroker) consume(

	ctx        context.Context,
	queueName  string,
	taskPrefix string,
	eta        ekatime.Timestamp,
) (
	[]map[string]interface{},
	*ekaerr.Error,
) {
	var (
		result     []string
		queueKey = p.buildKey(queueName, "")
	)

	if eta == 0 {
		p.consumeDelayed(ctx, queueName, 1*time.Second)

		result_, legacyErr := p.client.BRPop(1*time.Second, queueKey).Result()
		if legacyErr != nil && legacyErr != redis.Nil {
			return nil, ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to retrieve payloads (consume)").
				AddFields(
					"bokchoy_queue_key", queueKey,
					"bokchoy_queue_name", queueName,
					"bokchoy_task_prefix", taskPrefix,
					"bokchoy_error_redis_command", "BRPOP").
				Throw()
		}

		result = result_

	} else {

		results := p.client.ZRangeByScore(queueKey, &redis.ZRangeBy{
			Min: "0",
			Max: strconv.FormatInt(eta.I64(), 10),
		})

		if legacyErr := results.Err(); legacyErr != nil && legacyErr != redis.Nil {
			return nil, ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to retrieve payloads (consume)").
				AddFields(
					"bokchoy_queue_key", queueKey,
					"bokchoy_queue_name", queueName,
					"bokchoy_task_prefix", taskPrefix,
					"bokchoy_error_redis_command", "ZRANGEBYSCORE").
				Throw()
		}

		result = results.Val()
	}

	if len(result) == 0 {
		return nil, nil
	}

	taskKeys := make([]string, 0, len(result))
	for i, n := 0, len(result); i < n; i++ {
		if result[i] == queueName {
			continue
		}

		taskKeys = append(taskKeys, p.buildKey(taskPrefix, result[i]))
	}

	values, err := p.payloadsFromKeys(taskKeys)
	if err.IsNotNil() {
		return nil, err.
			AddMessage("Bokchoy: Failed to retrieve payloads (consume)").
			AddFields(
				"bokchoy_queue_key", queueKey,
				"bokchoy_queue_name", queueName,
				"bokchoy_task_prefix", taskPrefix).
			Throw()
	}

	results := make([]map[string]interface{}, 0, len(taskKeys))
	for _, data := range values {
		if len(data) == 0 {
			continue
		}

		results = append(results, data)
	}

	return results, nil
}

func (p *RedisBroker) publish(

	client    redis.Cmdable,
	queueName string,
	taskID    string,
	data      map[string]interface{},
	eta       ekatime.Timestamp,

) *ekaerr.Error {

	prefixedTaskKey := p.buildKey(queueName, taskID)

	legacyErr := client.HMSet(prefixedTaskKey, data).Err()
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to publish task. " +
				"Failed to save payload of task").
			AddFields(
				"bokchoy_task_key", prefixedTaskKey,
				"bokchoy_task_id", taskID,
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "HMSET").
			Throw()
	}

	if eta == 0 {

		legacyErr = client.RPush(p.buildKey(queueName, ""), taskID).Err()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to publish task. " +
					"Failed to add task to the queue").
				AddFields(
					"bokchoy_task_key", prefixedTaskKey,
					"bokchoy_task_id", taskID,
					"bokchoy_queue_name", queueName,
					"bokchoy_error_redis_command", "RPUSH").
				Throw()
		}

	} else if now := ekatime.Now(); eta <= now {
		// if eta is before now, then we should push this taskID in priority

		legacyErr = client.LPush(p.buildKey(queueName, ""), taskID).Err()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to publish task. " +
					"Failed to add task to the queue").
				AddFields(
					"bokchoy_task_key", prefixedTaskKey,
					"bokchoy_task_id", taskID,
					"bokchoy_queue_name", queueName,
					"bokchoy_error_redis_command", "LPUSH").
				Throw()
		}

	} else {

		legacyErr = client.ZAdd(p.buildKey(queueName + ":delay", ""), &redis.Z{
			Score:  float64(now),
			Member: taskID,
		}).Err()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to publish task. " +
					"Failed to add task to the queue").
				AddFields(
					"bokchoy_task_key", prefixedTaskKey,
					"bokchoy_task_id", taskID,
					"bokchoy_queue_name", queueName,
					"bokchoy_error_redis_command", "ZADD").
				Throw()
		}
	}

	return nil
}
