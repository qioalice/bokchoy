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
	"github.com/qioalice/ekago/v2/ekaerr"
)

func Init(options ...Option) *ekaerr.Error {

	defaultClient.sema.Lock()
	defer defaultClient.sema.Unlock()

	if defaultClient.isValid() {
		return ekaerr.InitializationFailed.
			New("Bokchoy: Already initialized. Do you call Init() twice?").
			Throw()
	}

	defaultClient_, err := New(options...)
	if err.IsNotNil() {
		return err.
			AddMessage("Bokchoy: Failed to initialize default client").
			Throw()
	}

	defaultClient = defaultClient_
	return nil
}

func GetQueue(name string, options ...Option) *Queue {
	return defaultClient.Queue(name, options...)
}

func Run() *ekaerr.Error {
	return defaultClient.Run().
		Throw()
}

func Stop() {
	defaultClient.Stop()
}

func Use(middlewares ...MiddlewareFunc) *Bokchoy {
	return defaultClient.Use(middlewares...)
}

func Empty() *ekaerr.Error {
	return defaultClient.Empty().
		Throw()
}

func ClearAll() *ekaerr.Error {
	return defaultClient.ClearAll().
		Throw()
}

func Publish(

	queueName string,
	payload   interface{},
	options   ...Option,
) (
	*Task,
	*ekaerr.Error,
) {
	task, err := defaultClient.Publish(queueName, payload, options...)
	return task, err.
		Throw()
}

func Handle(queueName string, callback HandlerFunc, options ...Option) {
	defaultClient.Handle(queueName, callback, options...)
}
