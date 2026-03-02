package trace

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infrago/infra"
	. "github.com/infrago/base"
)

func init() {
	infra.Mount(module)
}

var module = &Module{
	configs:   map[string]Config{},
	drivers:   map[string]Driver{},
	instances: map[string]*Instance{},
}

type (
	Module struct {
		mutex sync.RWMutex

		opened  bool
		started bool

		configs   map[string]Config
		drivers   map[string]Driver
		instances map[string]*Instance
		runners   map[string]*instanceRunner

		queue    chan Span
		stopChan chan struct{}
		doneChan chan struct{}

		queuedCount        atomic.Int64
		syncFallbackCount  atomic.Int64
		flushCount         atomic.Int64
		flushSpanCount     atomic.Int64
		writeErrorCount    atomic.Int64
		lastFlushLatencyMs atomic.Int64
		droppedCount       atomic.Int64
	}

	instanceRunner struct {
		name   string
		inst   *Instance
		queue  chan []Span
		stop   chan struct{}
		done   chan struct{}
		module *Module

		queuedCount        atomic.Int64
		flushCount         atomic.Int64
		flushSpanCount     atomic.Int64
		writeErrorCount    atomic.Int64
		lastWriteLatencyMs atomic.Int64
		droppedCount       atomic.Int64
	}

	Configs map[string]Config
	Config  struct {
		Driver  string
		Json    bool
		Buffer  int
		Timeout time.Duration
		Flag    string
		Format  string
		Sample  float64
		Fields  Any
		Setting Map
	}
)

func (m *Module) Register(name string, value Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	}
}

func (m *Module) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" {
		name = infra.DEFAULT
	}
	if driver == nil {
		panic(errInvalidTraceDriver)
	}
	if infra.Override() {
		m.drivers[name] = driver
	} else if _, ok := m.drivers[name]; !ok {
		m.drivers[name] = driver
	}
}

func (m *Module) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}
	if name == "" {
		name = infra.DEFAULT
	}
	if infra.Override() {
		m.configs[name] = cfg
	} else if _, ok := m.configs[name]; !ok {
		m.configs[name] = cfg
	}
}

func (m *Module) RegisterConfigs(configs Configs) {
	for name, cfg := range configs {
		m.RegisterConfig(name, cfg)
	}
}

func (m *Module) Config(global Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}
	cfgAny, ok := global["trace"]
	if !ok {
		return
	}
	cfgMap, ok := castMap(cfgAny)
	if !ok || cfgMap == nil {
		return
	}
	root := Map{}
	for key, val := range cfgMap {
		if key == "sample" {
			root[key] = val
			continue
		}
		if item, ok := castMap(val); ok && key != "setting" {
			m.configure(key, item)
		} else {
			root[key] = val
		}
	}
	if len(root) > 0 {
		m.configure(infra.DEFAULT, root)
	}
}

func (m *Module) configure(name string, conf Map) {
	cfg := Config{Driver: infra.DEFAULT, Sample: 1}
	if existing, ok := m.configs[name]; ok {
		cfg = existing
	}
	if v, ok := conf["driver"].(string); ok && v != "" {
		cfg.Driver = v
	}
	if v, ok := conf["json"].(bool); ok {
		cfg.Json = v
	}
	if v, ok := conf["flag"].(string); ok {
		cfg.Flag = v
	}
	if v, ok := conf["format"].(string); ok {
		cfg.Format = v
	}
	if v, ok := parseInt(conf["buffer"]); ok && v > 0 {
		cfg.Buffer = v
	}
	if v, ok := parseDuration(conf["timeout"]); ok && v > 0 {
		cfg.Timeout = v
	}
	if v, ok := parseFloat(conf["sample"]); ok {
		cfg.Sample = v
	}
	if v, ok := normalizeFieldsConfig(conf["fields"]); ok {
		cfg.Fields = v
	}
	if v, ok := castMap(conf["setting"]); ok {
		cfg.Setting = v
	}
	if cfg.Setting == nil {
		cfg.Setting = Map{}
	}

	if sampleMap, ok := castMap(conf["sample"]); ok && sampleMap != nil {
		applySampleConfig(&cfg, sampleMap)
	}

	m.configs[name] = cfg
}

func applySampleConfig(cfg *Config, sample Map) {
	if cfg == nil || sample == nil {
		return
	}
	if v, ok := parseFloat(sample["rate"]); ok {
		cfg.Sample = v
	}
	if v, ok := parseFloat(sample["sample"]); ok {
		cfg.Sample = v
	}
	if cfg.Setting == nil {
		cfg.Setting = Map{}
	}
	if v, ok := sample["error"].(bool); ok {
		cfg.Setting["sample_error"] = v
	}
	if v, ok := sample["key"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.Setting["sample_key"] = strings.TrimSpace(v)
	}
	if v, ok := sample["rules"]; ok {
		cfg.Setting["sample_rules"] = v
	}
	// backward compatible aliases
	if v, ok := sample["sample_error"].(bool); ok {
		cfg.Setting["sample_error"] = v
	}
	if v, ok := sample["sample_key"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.Setting["sample_key"] = strings.TrimSpace(v)
	}
	if v, ok := sample["sample_rules"]; ok {
		cfg.Setting["sample_rules"] = v
	}
}

func normalizeFieldsConfig(raw Any) (Any, bool) {
	switch vv := raw.(type) {
	case nil:
		return nil, false
	case []string:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, strings.TrimSpace(one))
		}
		return out, true
	case []Any:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, strings.TrimSpace(fmt.Sprintf("%v", one)))
		}
		return out, true
	case Map:
		out := Map{}
		for k, v := range vv {
			key := strings.TrimSpace(k)
			val := strings.TrimSpace(fmt.Sprintf("%v", v))
			if key == "" || val == "" {
				continue
			}
			out[key] = val
		}
		return out, true
	default:
		return nil, false
	}
}

func (m *Module) Setup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}
	if len(m.configs) == 0 {
		m.configs[infra.DEFAULT] = normalizeConfig(Config{Driver: infra.DEFAULT, Sample: 1})
		return
	}
	for name, cfg := range m.configs {
		m.configs[name] = normalizeConfig(cfg)
	}
}

func (m *Module) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.opened {
		return
	}
	for name, cfg := range m.configs {
		drv := m.drivers[cfg.Driver]
		if drv == nil {
			panic("invalid trace driver: " + cfg.Driver)
		}
		inst := &Instance{Name: name, Config: cfg, Setting: cfg.Setting}
		conn, err := drv.Connect(inst)
		if err != nil {
			panic("failed to connect trace: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("failed to open trace: " + err.Error())
		}
		inst.conn = conn
		m.instances[name] = inst
	}
	m.runners = map[string]*instanceRunner{}
	m.opened = true
}

func (m *Module) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.started {
		return
	}
	bufferSize := 1024
	flushEvery := 200 * time.Millisecond
	for _, inst := range m.instances {
		if inst.Config.Buffer > bufferSize {
			bufferSize = inst.Config.Buffer
		}
		if inst.Config.Timeout > 0 && inst.Config.Timeout < flushEvery {
			flushEvery = inst.Config.Timeout
		}
	}
	m.queue = make(chan Span, bufferSize)
	m.stopChan = make(chan struct{})
	m.doneChan = make(chan struct{})
	m.queuedCount.Store(0)
	m.syncFallbackCount.Store(0)
	m.flushCount.Store(0)
	m.flushSpanCount.Store(0)
	m.writeErrorCount.Store(0)
	m.lastFlushLatencyMs.Store(0)
	m.droppedCount.Store(0)

	m.runners = map[string]*instanceRunner{}
	for name, inst := range m.instances {
		queueSize := inst.Config.Buffer
		if queueSize <= 0 {
			queueSize = 1024
		}
		runner := &instanceRunner{
			name:   name,
			inst:   inst,
			queue:  make(chan []Span, queueSize),
			stop:   make(chan struct{}),
			done:   make(chan struct{}),
			module: m,
		}
		m.runners[name] = runner
		go runner.loop()
	}
	go m.loop(flushEvery)
	m.started = true
	fmt.Printf("infrago trace module is running with %d connections.\n", len(m.instances))
}

func (m *Module) Stop() {
	m.mutex.Lock()
	if !m.started {
		m.mutex.Unlock()
		return
	}
	stop, done := m.stopChan, m.doneChan
	m.started = false
	runners := make([]*instanceRunner, 0, len(m.runners))
	for _, runner := range m.runners {
		runners = append(runners, runner)
	}
	m.mutex.Unlock()
	close(stop)
	<-done
	for _, runner := range runners {
		close(runner.stop)
		<-runner.done
	}
}

func (m *Module) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.opened {
		return
	}
	for _, inst := range m.instances {
		_ = inst.conn.Close()
	}
	m.instances = map[string]*Instance{}
	m.runners = map[string]*instanceRunner{}
	m.opened = false
}

func (m *Module) loop(flushEvery time.Duration) {
	defer close(m.doneChan)
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()
	batch := make([]Span, 0, 256)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		_, dropped := m.dispatch(batch)
		m.flushCount.Add(1)
		m.flushSpanCount.Add(int64(len(batch)))
		m.droppedCount.Add(int64(dropped))
		m.lastFlushLatencyMs.Store(time.Since(start).Milliseconds())
		batch = batch[:0]
	}
	for {
		select {
		case entry := <-m.queue:
			batch = append(batch, entry)
			if len(batch) >= cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-m.stopChan:
			for {
				select {
				case entry := <-m.queue:
					batch = append(batch, entry)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (m *Module) dispatch(spans []Span) (int, int) {
	m.mutex.RLock()
	runners := make([]*instanceRunner, 0, len(m.runners))
	queueLen := 0
	queueCap := 0
	if m.queue != nil {
		queueLen = len(m.queue)
		queueCap = cap(m.queue)
	}
	for _, runner := range m.runners {
		runners = append(runners, runner)
	}
	m.mutex.RUnlock()
	factor := m.dynamicSampleFactor(queueLen, queueCap)
	dropped := 0
	for _, runner := range runners {
		filtered := make([]Span, 0, len(spans))
		for _, span := range spans {
			if runner.inst.AllowWithFactor(span, factor) {
				filtered = append(filtered, span)
			} else {
				dropped++
			}
		}
		if len(filtered) == 0 {
			continue
		}
		select {
		case runner.queue <- filtered:
			runner.queuedCount.Add(int64(len(filtered)))
		default:
			dropped += len(filtered)
			runner.droppedCount.Add(int64(len(filtered)))
		}
	}
	return len(spans), dropped
}

func (m *Module) Write(span Span) {
	if span.Time.IsZero() {
		span.Time = time.Now()
	}
	if span.Status == StatusError {
		span.Status = StatusFail
	}
	if span.Status == "" {
		if span.Code == 0 {
			span.Status = StatusOK
		} else {
			span.Status = StatusFail
		}
	}
	if span.Code == 0 && span.Status != "" && span.Status != StatusOK {
		span.Code = 1
	}
	if span.Start <= 0 {
		span.Start = span.Time.UnixNano()
	}
	if span.End <= 0 {
		span.End = span.Time.UnixNano()
	}
	if span.Duration < 0 {
		span.Duration = 0
	}
	if span.Duration == 0 && span.End >= span.Start {
		span.Duration = span.End - span.Start
	}
	if span.Attributes == nil {
		span.Attributes = Map{}
	}

	m.mutex.RLock()
	started := m.started
	queue := m.queue
	m.mutex.RUnlock()
	if started && queue != nil {
		select {
		case queue <- span:
			m.queuedCount.Add(1)
			return
		default:
		}
	}
	m.syncFallbackCount.Add(1)
	_, dropped := m.dispatch([]Span{span})
	m.flushCount.Add(1)
	m.flushSpanCount.Add(1)
	m.droppedCount.Add(int64(dropped))
	m.lastFlushLatencyMs.Store(0)
}

func (m *Module) dynamicSampleFactor(queueLen, queueCap int) float64 {
	factor := 1.0
	if queueCap > 0 {
		load := float64(queueLen) / float64(queueCap)
		switch {
		case load >= 0.95:
			factor = 0.10
		case load >= 0.90:
			factor = 0.20
		case load >= 0.80:
			factor = 0.50
		}
	}
	return factor
}

func (m *Module) Stats() Map {
	m.mutex.RLock()
	queueLen := 0
	queueCap := 0
	started := m.started
	if m.queue != nil {
		queueLen = len(m.queue)
		queueCap = cap(m.queue)
	}
	instances := len(m.instances)
	runners := make([]*instanceRunner, 0, len(m.runners))
	for _, runner := range m.runners {
		runners = append(runners, runner)
	}
	m.mutex.RUnlock()

	connections := Map{}
	for _, runner := range runners {
		connections[runner.name] = Map{
			"queue_len":             len(runner.queue),
			"queue_cap":             cap(runner.queue),
			"queued_count":          runner.queuedCount.Load(),
			"flush_count":           runner.flushCount.Load(),
			"flush_span_count":      runner.flushSpanCount.Load(),
			"write_error_count":     runner.writeErrorCount.Load(),
			"last_write_latency_ms": runner.lastWriteLatencyMs.Load(),
			"dropped_count":         runner.droppedCount.Load(),
		}
	}

	return Map{
		"started":               started,
		"connection_count":      instances,
		"connections":           connections,
		"queue_len":             queueLen,
		"queue_cap":             queueCap,
		"queued_count":          m.queuedCount.Load(),
		"sync_fallback_count":   m.syncFallbackCount.Load(),
		"flush_count":           m.flushCount.Load(),
		"flush_span_count":      m.flushSpanCount.Load(),
		"write_error_count":     m.writeErrorCount.Load(),
		"last_flush_latency_ms": m.lastFlushLatencyMs.Load(),
		"dropped_count":         m.droppedCount.Load(),
		"dynamic_sample_factor": m.dynamicSampleFactor(queueLen, queueCap),
	}
}

func (r *instanceRunner) loop() {
	defer close(r.done)
	for {
		select {
		case batch := <-r.queue:
			r.write(batch)
		case <-r.stop:
			for {
				select {
				case batch := <-r.queue:
					r.write(batch)
				default:
					return
				}
			}
		}
	}
}

func (r *instanceRunner) write(batch []Span) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()
	r.flushCount.Add(1)
	r.flushSpanCount.Add(int64(len(batch)))
	if err := r.inst.conn.Write(batch...); err != nil {
		r.module.writeErrorCount.Add(1)
		r.writeErrorCount.Add(1)
		_, _ = fmt.Fprintln(os.Stderr, "trace write failed:", err.Error())
	}
	r.lastWriteLatencyMs.Store(time.Since(start).Milliseconds())
}

func (m *Module) Begin(meta *infra.Meta, name string, attrs Map) infra.TraceSpan {
	return Begin(meta, name, attrs)
}

func (m *Module) Trace(meta *infra.Meta, name string, status string, attrs Map) error {
	h := Begin(meta, name, attrs)
	if h == nil {
		return nil
	}
	if status != "" {
		h.span.Status = strings.ToLower(strings.TrimSpace(status))
		if h.span.Status == StatusOK {
			h.span.Code = 0
		} else {
			h.span.Code = 1
		}
	}
	if h.span.Status != StatusOK {
		if attrs != nil {
			if errText, ok := attrs["error"].(string); ok && errText != "" {
				h.span.Result = errText
			}
		}
	}
	h.End()
	return nil
}

func castMap(value Any) (Map, bool) {
	v, ok := value.(Map)
	return v, ok
}

func parseInt(value Any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(v)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func parseFloat(value Any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func parseDuration(value Any) (time.Duration, bool) {
	switch v := value.(type) {
	case time.Duration:
		return v, true
	case int:
		return time.Second * time.Duration(v), true
	case int64:
		return time.Second * time.Duration(v), true
	case float64:
		return time.Second * time.Duration(v), true
	case string:
		if d, err := time.ParseDuration(v); err == nil {
			return d, true
		}
	}
	return 0, false
}
