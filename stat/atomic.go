package stat

import(
	"sync"
	"sync/atomic"
	"encoding/json"
	"fmt"
)

type AtomicFloat struct {
	v float64
	mu sync.RWMutex
}

func (s *AtomicFloat) Add(val float64) {
	s.mu.Lock()
	s.v += val
	s.mu.Unlock()
}

func (s *AtomicFloat) Get() float64 {
	s.mu.Lock()
	v := s.v
	s.mu.Unlock()
	return v
}

func (s *AtomicFloat) Set(val float64) {
	s.mu.Lock()
	s.v = val
	s.mu.Unlock()
}

func (s *AtomicFloat) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Get())
}

type AtomicInt struct {
	v int64
}

func (s *AtomicInt) Add(val int64) {
	atomic.AddInt64(&s.v, val)
}

func (s *AtomicInt) Increment() {
	atomic.AddInt64(&s.v, 1)
}

func (s *AtomicInt) Decrement() {
	atomic.AddInt64(&s.v, -1)
}

func (s *AtomicInt) Get() int64 {
	return atomic.LoadInt64(&s.v)
}

func (s *AtomicInt) Set(val int64) {
	atomic.StoreInt64(&s.v, val)
}

func (s *AtomicInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Get())
}

type AtomicString struct {
	v string
	mu sync.RWMutex
}

func (s *AtomicString) Set(val string) {
	s.mu.Lock()
	s.v = val
	s.mu.Unlock()
}

func (s *AtomicString) Get() string {
	s.mu.RLock()
	result := s.v
	s.mu.RUnlock()

	return result
}

func (s *AtomicString) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Get())
}

func NewAtomicMap() *AtomicMap{
	return &AtomicMap{
		v: make(map[string]interface{}),
	}
}

type AtomicMap struct {
	v map[string]interface{}
	mu sync.RWMutex
}

func (s *AtomicMap) Get(key string) interface{} {
	var result interface{}
	s.mu.RLock()
	if v, ok := s.v[key]; ok {
		result = v
	}
	s.mu.RUnlock()
	return result
}

func (s *AtomicMap) Add(key string, value int64){
	s.mu.Lock()
	if _, ok := s.v[key]; !ok {
		s.v[key] = &AtomicInt{}
	}
	if m, ok := s.v[key].(*AtomicInt); ok {
		m.Add(value)
	} else {
		panic(fmt.Sprintf("atomic_map: umatched type for %s", key))
	}

	s.mu.Unlock()
}

func (s *AtomicMap) AddFloat(key string, value float64){
	s.mu.Lock()
	if _, ok := s.v[key]; !ok {
		s.v[key] = &AtomicFloat{}
	}
	if m, ok := s.v[key].(*AtomicFloat); ok {
		m.Add(value)
	} else {
		panic(fmt.Sprintf("atomic_map: umatched type for %s", key))
	}
	s.mu.Unlock()
}

func (s *AtomicMap) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	result := make(map[string]interface{})
	for key, value := range s.v {
		if v, ok := value.(*AtomicInt); ok {
			result[key] = v.Get()
		} else if v, ok := value.(*AtomicFloat); ok {
			result[key] = v.Get()
		} else {
			panic("unsupported type")
		}
	}
	s.mu.RUnlock()
	return json.Marshal(result)
}
