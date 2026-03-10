package trace

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Instance struct {
	conn Connection

	Name    string
	Config  Config
	Setting map[string]any

	sampleOnce   sync.Once
	samplePolicy samplePolicy
}

func (inst *Instance) Allow(span Span) bool {
	return inst.AllowWithFactor(span, 1)
}

func (inst *Instance) AllowWithFactor(span Span, factor float64) bool {
	if inst == nil {
		return false
	}
	inst.sampleOnce.Do(func() {
		inst.samplePolicy = buildSamplePolicy(inst.Setting)
	})
	r := chooseSampleRatio(span, inst.Config.Sample, inst.samplePolicy)
	r *= clamp01(factor)
	r = clamp01(r)
	if r >= 1 {
		return true
	}
	if r <= 0 {
		return false
	}
	x := hash01(sampleKey(span, inst.samplePolicy.HashBy))
	return x <= r
}

func (inst *Instance) Format(span Span) string {
	if inst == nil {
		return ""
	}
	values := SpanValues(span, inst.Name, inst.Config.Flag)
	fields := ResolveFields(inst.Config.Fields, defaultOutputFields())
	if inst.Config.Json {
		dataMap := map[string]any{}
		for source, target := range fields {
			if target == "" {
				continue
			}
			if val, ok := values[source]; ok {
				dataMap[target] = val
			}
		}
		data, _ := json.Marshal(dataMap)
		return string(data)
	}

	message := inst.Config.Format
	if message == "" {
		message = "%ts% [%status%] code=%code% kind=%kind% entry=%entry% step=%step% trace=%traceId% span=%spanId% duration=%duration%ns"
	}
	for key, val := range values {
		message = strings.ReplaceAll(message, "%"+key+"%", fmt.Sprintf("%v", val))
	}
	for source, target := range fields {
		if target == "" {
			continue
		}
		if val, ok := values[source]; ok {
			message = strings.ReplaceAll(message, "%"+target+"%", fmt.Sprintf("%v", val))
		}
	}
	return message
}

func defaultOutputFields() map[string]string {
	return map[string]string{
		"time":           "time",
		"start":          "start",
		"end":            "end",
		"duration":       "duration",
		"duration_nano":  "duration_nano",
		"trace_id":       "trace_id",
		"span_id":        "span_id",
		"parent_span_id": "parent_span_id",
		"step":           "step",
		"entry":          "entry",
		"kind":           "kind",
		"status":         "status",
		"code":           "code",
		"result":         "result",
		"attributes":     "attributes",
		"resource":       "resource",
		"project":        "project",
		"role":           "role",
		"profile":        "profile",
		"node":           "node",
		"flag":           "flag",
	}
}

func normalizeConfig(cfg Config) Config {
	if cfg.Driver == "" {
		cfg.Driver = "default"
	}
	if cfg.Buffer <= 0 {
		cfg.Buffer = 1024
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 200 * time.Millisecond
	}
	if cfg.Format == "" {
		cfg.Format = "%ts% [%status%] code=%code% kind=%kind% entry=%entry% step=%step% trace=%traceId% span=%spanId% duration=%duration%ns"
	}
	if cfg.Sample < 0 {
		cfg.Sample = 1
	}
	if cfg.Sample > 1 {
		cfg.Sample = 1
	}
	return cfg
}
