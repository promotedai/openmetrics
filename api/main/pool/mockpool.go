// Forked and modified from
// https://github.com/goinaction/code/blob/master/chapter7/patterns/pool/pool.go

package pool

type MockPool struct {
	Resource interface{}
}

// Acquires resource from pool.
func (p *MockPool) Acquire() (interface{}, error) {
	return p.Resource, nil
}

// Release places a new resource onto the pool.
func (p *MockPool) Release(r interface{}) error {
	return nil
}

// Close will shutdown the pool and close all existing resources.
func (p *MockPool) Close() error {
	return nil
}
