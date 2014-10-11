package stat

import (
	"time"
	"runtime"
	"sync"
	"fmt"
	"github.com/influxdb/influxdb/configuration"
)

type ServerMetrics struct {
	Name *AtomicString `json:"name"`
	Version *AtomicString `json:"version"`
	Pid *AtomicInt `json:"pid"`
	Uptime *AtomicInt `json:"uptime"`
	Api apiMetrics `json:"api"`
	Go goMetrics `json:"go"`
	Wal walMetrics `json:"wal"`
	Shard shardMetrics `json:"shard"`
	Coordinator coordinatorMetrics `json:"coordinator"`

	// store custom metrics under custom namespace.
	// you can store your own metrics (for example, company specific api) here.
	Custom map[string]interface{} `json:"custom"`

	mu sync.RWMutex
	boot time.Time `json:"-"`
	once sync.Once
}

func (s *ServerMetrics) Update(config *configuration.Configuration) {
	s.once.Do(func() {
		s.Name.Set(config.HostnameOrDetect())
	})

	s.Uptime.Set(int64(time.Now().Sub(Metrics.boot).Seconds()))
	s.Go.CgoCall.Set(runtime.NumCgoCall())
	s.Go.NumGoroutine.Set(int64(runtime.NumGoroutine()))
}

func (s *ServerMetrics) Register(key string, v interface{}) {
	s.mu.Lock()
	if _, ok := s.Custom[key]; !ok {
		s.Custom[key] = v
	} else {
		panic(fmt.Sprintf("%s key has already registered", key))
	}
	s.mu.Unlock()
}

func (s *ServerMetrics) Get(key string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.Custom[key]; !ok {
		return v
	} else {
		panic(fmt.Sprintf("%s key doesn't find", key))
	}
}


type apiMetrics struct {
	Http httpMetrics `json:"http"`
}

type goMetrics struct {
	CgoCall *AtomicInt `json:"cgo_call"`
	NumGoroutine *AtomicInt `json:"num_goroutine"`
}

type walMetrics struct {
	Opening *AtomicInt `json:"opening"`
	CloseEntry *AtomicInt `json:"close_entry"`
	BookmarkEntry *AtomicInt `json:"bookmark_entry"`
	CommitEntry *AtomicInt `json:"commit_entry"`
	AppendEntry *AtomicInt `json:"append_entry"`
}

type coordinatorMetrics struct {
	CmdDropContinuousQuery *AtomicInt `json:"cmd_drop_continuous_query"`
	CmdListContinuousQuery *AtomicInt `json:"cmd_list_continuous_query"`
	CmdContinuousQuery *AtomicInt `json:"cmd_continuous_query"`
	CmdQuery *AtomicInt `json:"cmd_query"`
	CmdSelect *AtomicInt `json:"cmd_select"`
	CmdWriteSeries *AtomicInt `json:"cmd_write_series"`
	CmdDelete *AtomicInt `json:"cmd_delete"`
	CmdDropSeries *AtomicInt `json:"cmd_drop_series"`
	CmdListSeries *AtomicInt `json:"cmd_list_series"`
}

type shardMetrics struct {
	Opening *AtomicInt `json:"opening"`
	Delete *AtomicInt `json:"delete"`
}

type httpMetrics struct {
	Status *AtomicMap `json:"status"`
}

// Basic concept:
//
// Stat stores influx specific internal metrics such as wal.commit_entry, num goroutines.
// It Shouldn't store OS related values (load average, os free memory)
// Currently, we provides basic metrics only. please send a patch if you need more detail metrics.
//
// We don't provide periodical update yet. Just call `UpdateMetrics` function before exporting metrics data.
//
var Metrics *ServerMetrics = &ServerMetrics{
	Name: &AtomicString{},
	Version: &AtomicString{},
	Pid: &AtomicInt{},
	Uptime: &AtomicInt{},

	Api: apiMetrics{
		Http: httpMetrics{
			Status: NewAtomicMap(),
		},
	},
	Wal: walMetrics{
		Opening: &AtomicInt{},
		CloseEntry: &AtomicInt{},
		BookmarkEntry: &AtomicInt{},
		CommitEntry: &AtomicInt{},
		AppendEntry: &AtomicInt{},
	},
	Go: goMetrics{
		CgoCall: &AtomicInt{},
		NumGoroutine: &AtomicInt{},
	},
	Shard: shardMetrics{
		Opening: &AtomicInt{},
		Delete: &AtomicInt{},
	},
	Coordinator: coordinatorMetrics{
		CmdQuery: &AtomicInt{},
		CmdDropContinuousQuery: &AtomicInt{},
		CmdListContinuousQuery: &AtomicInt{},
		CmdContinuousQuery: &AtomicInt{},
		CmdSelect: &AtomicInt{},
		CmdWriteSeries: &AtomicInt{},
		CmdDelete: &AtomicInt{},
		CmdDropSeries: &AtomicInt{},
		CmdListSeries: &AtomicInt{},
	},
	Custom: make(map[string]interface{}),
}

func init() {
	Metrics.boot = time.Now()
}
