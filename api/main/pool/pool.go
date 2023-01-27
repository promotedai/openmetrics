// Forked and modified from
// https://github.com/goinaction/code/blob/master/chapter7/patterns/pool/pool.go

package pool

import (
	"errors"
	"sync"
)

type Pool interface {
	Acquire() (interface{}, error)
	Release(r interface{}) error
	Close() error
}

// This is a weird pool implementation.  Not intentional.
// I just didn't see a great, simple pool impl.
// size is actually how many released resources to keep around.
// The pool can grow unbounded.
// It does not have a max.
type PoolImpl struct {
	m         sync.Mutex
	resources chan interface{}
	open      func() (interface{}, error)
	close     func(interface{}) error
	closed    bool
}

var ErrPoolClosed = errors.New("Pool has been closed.")

// release_min_size - don't shrink below this size.
func NewPool(
	open func() (interface{}, error),
	close func(interface{}) error,
	releaseMinSize uint) (Pool, error) {
	if releaseMinSize <= 0 {
		return nil, errors.New("releaseMinSize needs to be positive.")
	}
	return &PoolImpl{
		open:      open,
		close:     close,
		resources: make(chan interface{}, releaseMinSize),
	}, nil
}

// Acquires resource from pool.
func (p *PoolImpl) Acquire() (interface{}, error) {
	select {
	// Check for a free resource.
	case r, ok := <-p.resources:
		if !ok {
			return nil, ErrPoolClosed
		}
		return r, nil

	// Create resource
	default:
		return p.open()
	}
}

// Release places a new resource onto the pool.
func (p *PoolImpl) Release(r interface{}) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.closed {
		return p.close(r)
	}

	select {
	// Attempt to place the new resource on the queue.
	case p.resources <- r:
		// Do nothing.
	// If the queue is already at cap we close the resource.
	default:
		return p.close(r)
	}
	return nil
}

// Close will shutdown the pool and close all existing resources.
func (p *PoolImpl) Close() error {
	p.m.Lock()
	defer p.m.Unlock()

	// If the pool is already close, don't do anything.
	if p.closed {
		return nil
	}
	p.closed = true

	// Close the channel before we drain the channel of its
	// resources. If we don't do this, we will have a deadlock.
	close(p.resources)

	// Close the resources
	var err error
	for r := range p.resources {
		err2 := p.close(r)
		if err2 != nil {
			err = err2
		}
	}
	return err
}
