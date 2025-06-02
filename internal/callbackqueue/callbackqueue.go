package callbackqueue

import (
	"fmt"
	"sync"

	"github.com/lorenzodonini/ocpp-go/ocpp"
)

type CallbackQueue struct {
	callbacksMutex sync.RWMutex
	callbacks      map[string][]func(confirmation ocpp.Response, err error)
}

func New() CallbackQueue {
	return CallbackQueue{
		callbacks: make(map[string][]func(confirmation ocpp.Response, err error)),
	}
}

func (cq *CallbackQueue) TryQueue(id string, try func() error, callback func(confirmation ocpp.Response, err error)) error {
	cq.callbacksMutex.Lock()
	defer cq.callbacksMutex.Unlock()

	cq.callbacks[id] = append(cq.callbacks[id], callback)

	if err := try(); err != nil {
		// pop off last element
		callbacks := cq.callbacks[id]
		cq.callbacks[id] = callbacks[:len(callbacks)-1]
		if len(cq.callbacks[id]) == 0 {
			delete(cq.callbacks, id)
		}

		return err
	}

	return nil
}

func (cq *CallbackQueue) Dequeue(id string) (func(confirmation ocpp.Response, err error), bool) {
	cq.callbacksMutex.Lock()
	defer cq.callbacksMutex.Unlock()

	callbacks, ok := cq.callbacks[id]
	if !ok {
		return nil, false
	}

	if len(callbacks) == 0 {
		panic("Internal CallbackQueue inconsistency")
	}

	callback := callbacks[0]

	if len(callbacks) == 1 {
		delete(cq.callbacks, id)
	} else {
		cq.callbacks[id] = callbacks[1:]
	}

	return callback, ok
}

// CheckHealth returns diagnostic information about the callback queue's current state
func (cq *CallbackQueue) CheckHealth() string {
	cq.callbacksMutex.RLock()
	defer cq.callbacksMutex.RUnlock()

	totalCallbacks := 0
	clientDetails := ""

	for clientID, callbacks := range cq.callbacks {
		count := len(callbacks)
		totalCallbacks += count
		if count > 0 {
			clientDetails += fmt.Sprintf("\n    - Client %s: %d pending", clientID, count)
		}
	}

	return fmt.Sprintf("CallbackQueue: totalPending=%d, clientCount=%d%s",
		totalCallbacks, len(cq.callbacks), clientDetails)
}
