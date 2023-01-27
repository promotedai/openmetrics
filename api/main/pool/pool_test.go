package pool // "github.com/promotedai/metrics/api/main/pool"

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockResource struct {
	i int
}

// These are not thorough tests.  Just enough to test dumb mistakes.

func TestAcquire(t *testing.T) {
	i := 0

	pool, err := NewPool(
		func() (interface{}, error) {
			r := &MockResource{
				i: i,
			}
			i++
			return r, nil
		},
		func(_ interface{}) error {
			return nil
		},
		2)
	assert.Nil(t, err)

	assert.Equal(t, 0, acquire(t, pool).i)
	assert.Equal(t, 1, acquire(t, pool).i)
	assert.Equal(t, 2, acquire(t, pool).i)
	assert.Equal(t, 3, acquire(t, pool).i)
	assert.Equal(t, 4, acquire(t, pool).i)
}

func TestAcquireAndRelease(t *testing.T) {
	i := 0

	pool, err := NewPool(
		func() (interface{}, error) {
			r := &MockResource{
				i: i,
			}
			i++
			return r, nil
		},
		func(_ interface{}) error {
			return nil
		},
		2)
	assert.Nil(t, err)

	r0 := acquire(t, pool)
	assert.Equal(t, 0, r0.i)
	r1 := acquire(t, pool)
	assert.Equal(t, 1, r1.i)
	r2 := acquire(t, pool)
	assert.Equal(t, 2, r2.i)
	r3 := acquire(t, pool)
	assert.Equal(t, 3, r3.i)
	r4 := acquire(t, pool)
	assert.Equal(t, 4, r4.i)
	pool.Release(r0)
	pool.Release(r1)
	pool.Release(r2)
	pool.Release(r3)
	pool.Release(r4)
	r5 := acquire(t, pool)
	assert.Equal(t, 0, r5.i)
	r6 := acquire(t, pool)
	assert.Equal(t, 1, r6.i)
	r7 := acquire(t, pool)
	assert.Equal(t, 5, r7.i)
	r8 := acquire(t, pool)
	assert.Equal(t, 6, r8.i)
	pool.Release(r6)
	pool.Release(r7)
	pool.Release(r5)
	pool.Release(r8)
	r9 := acquire(t, pool)
	assert.Equal(t, 1, r9.i)
	r10 := acquire(t, pool)
	assert.Equal(t, 5, r10.i)
	r11 := acquire(t, pool)
	assert.Equal(t, 7, r11.i)
	r12 := acquire(t, pool)
	assert.Equal(t, 8, r12.i)
}

func acquire(t *testing.T, pool Pool) *MockResource {
	r, err := pool.Acquire()
	assert.Nil(t, err)
	return r.(*MockResource)
}

// Just make sure it doesn't blow up.
func TestClose(t *testing.T) {
	i := 0

	pool, err := NewPool(
		func() (interface{}, error) {
			r := &MockResource{
				i: i,
			}
			i++
			return r, nil
		},
		func(_ interface{}) error {
			return nil
		},
		2)
	assert.Nil(t, err)

	r0 := acquire(t, pool)
	r1 := acquire(t, pool)
	pool.Release(r0)
	pool.Release(r1)

	pool.Close()
}
