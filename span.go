package trace

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/infrago/infra"
	. "github.com/infrago/base"
)

type Span struct {
	Time         time.Time `json:"time"`
	TraceId      string    `json:"trace_id,omitempty"`
	SpanId       string    `json:"span_id,omitempty"`
	ParentSpanId string    `json:"parent_span_id,omitempty"`
	Name         string    `json:"name,omitempty"`
	Kind         string    `json:"kind,omitempty"`
	ServiceName  string    `json:"service_name,omitempty"`
	Target       string    `json:"target,omitempty"`
	Status       string    `json:"status,omitempty"`
	Code         int       `json:"code"`
	Result       string    `json:"result,omitempty"`
	Duration     int64     `json:"duration"`
	Start        int64     `json:"start"`
	End          int64     `json:"end"`
	Attributes   Map       `json:"attributes,omitempty"`
	Resource     Map       `json:"resource,omitempty"`
}

type Handle struct {
	meta  *infra.Meta
	span  Span
	start time.Time
}

var spanSeq atomic.Uint64

func Begin(meta *infra.Meta, name string, attrs ...Map) *Handle {
	if meta == nil {
		meta = infra.NewMeta()
	}
	traceId := meta.TraceId()
	if traceId == "" {
		traceId = nextId("tr")
		meta.TraceId(traceId)
	}
	parent := meta.SpanId()
	spanId := nextId("sp")
	prevParent := meta.ParentSpanId()
	meta.ParentSpanId(parent)
	meta.SpanId(spanId)
	meta.PushSpanFrame(parent, prevParent)

	now := time.Now()
	span := Span{
		TraceId:      traceId,
		SpanId:       spanId,
		ParentSpanId: parent,
		Name:         name,
		Status:       StatusOK,
		Code:         0,
		Time:         now,
		Start:        now.UnixNano(),
		Attributes:   Map{},
		Resource:     Map{},
	}
	identity := infra.Identity()
	span.Resource["infra.project"] = identity.Project
	span.Resource["infra.profile"] = identity.Profile
	span.Resource["infra.node"] = identity.Node
	if len(attrs) > 0 && attrs[0] != nil {
		for k, v := range attrs[0] {
			span.Attributes[k] = v
		}
		if v, ok := attrs[0]["kind"].(string); ok {
			span.Kind = v
		}
		if v, ok := attrs[0]["service"].(string); ok {
			span.ServiceName = v
			span.Resource["service.name"] = v
		}
		if v, ok := attrs[0]["entry"].(string); ok {
			span.Target = v
		}
		if v, ok := attrs[0]["step"].(string); ok && v != "" {
			span.Name = v
		}
		if v, ok := attrs[0]["status"].(string); ok && v != "" {
			span.Status = strings.ToLower(strings.TrimSpace(v))
		}
		if v, ok := attrs[0]["target"].(string); ok {
			span.Target = v
		}
		if v, ok := parseCode(attrs[0]["code"]); ok {
			span.Code = v
		}
		if v, ok := attrs[0]["result"].(string); ok && v != "" {
			span.Result = v
		}
	}

	if span.Kind == "" {
		span.Kind = strings.TrimSpace(meta.TraceKind())
	}
	if span.Target == "" {
		span.Target = strings.TrimSpace(meta.TraceEntry())
	}
	if span.Kind != "" {
		meta.TraceKind(span.Kind)
	}
	if span.Target != "" {
		meta.TraceEntry(span.Target)
	}

	return &Handle{
		meta:  meta,
		span:  span,
		start: time.Now(),
	}
}

func (h *Handle) End(results ...Any) {
	if h == nil {
		return
	}
	now := time.Now()
	h.span.Time = now
	h.span.End = now.UnixNano()
	h.span.Duration = time.Since(h.start).Nanoseconds()
	okCode := infra.OK.Code()
	okStatus := strings.ToLower(strings.TrimSpace(infra.OK.Status()))
	if okStatus == "" {
		okStatus = StatusOK
	}
	failCode := infra.Fail.Code()
	failStatus := strings.ToLower(strings.TrimSpace(infra.Fail.Status()))
	if failStatus == "" {
		failStatus = StatusFail
	}

	// Default: success.
	h.span.Code = okCode
	h.span.Status = okStatus
	h.span.Result = ""

	var first Any
	for _, item := range results {
		if item != nil {
			first = item
			break
		}
	}
	if first != nil {
		switch v := first.(type) {
		case Res:
			h.span.Code = v.Code()
			h.span.Status = strings.ToLower(strings.TrimSpace(v.Status()))
			if h.span.Status == "" {
				if h.span.Code == okCode {
					h.span.Status = okStatus
				} else {
					h.span.Status = failStatus
				}
			}
			h.span.Result = v.Error()
		case error:
			h.span.Code = failCode
			h.span.Status = failStatus
			h.span.Result = v.Error()
		default:
			h.span.Code = failCode
			h.span.Status = failStatus
			h.span.Result = fmt.Sprintf("%v", v)
		}
	}
	Write(h.span)
	if h.meta != nil {
		if prevSpanId, prevParent, ok := h.meta.PopSpanFrame(); ok {
			h.meta.SpanId(prevSpanId)
			h.meta.ParentSpanId(prevParent)
		}
	}
}

func Emit(meta *infra.Meta, name string, status string, attrs ...Map) {
	h := Begin(meta, name, attrs...)
	if h == nil {
		return
	}
	if status != "" {
		h.span.Status = status
		if status == StatusOK {
			h.span.Code = 0
		} else {
			h.span.Code = 1
		}
	}
	h.End()
}

func nextId(prefix string) string {
	n := spanSeq.Add(1)
	return fmt.Sprintf("%s%x%x", prefix, time.Now().UnixNano(), n)
}

func hash01(s string) float64 {
	if s == "" {
		return 1
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	v := h.Sum64()
	return float64(v%1000000) / 1000000.0
}

func parseCode(v Any) (int, bool) {
	switch vv := v.(type) {
	case int:
		return vv, true
	case int64:
		return int(vv), true
	case float64:
		return int(vv), true
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil {
			return n, true
		}
	}
	return 0, false
}
