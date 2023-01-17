package trace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/common"
)

type tracev struct {
	Name  string         `json:"name"`
	Cat   string         `json:"cat,omitempty"`
	Ident int32          `json:"id,omitempty"`
	T     string         `json:"ph"`
	Ts    int64          `json:"ts,omitempty"`
	Pid   int64          `json:"pid"`
	Tid   int64          `json:"tid,omitempty"`
	Args  map[string]any `json:"args,omitempty"`
}

var T = false

func ToFile(path string) {
	DefaultTrace = NewTraceToFile(path)
	T = true
}

var DefaultTrace *Trace

func NewTraceToFile(path string) *Trace {
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	bufw := bufio.NewWriter(file)
	_, _ = bufw.WriteString("[\n")

	return &Trace{
		ticks:  new(atomic.Int64),
		closer: file,
		out:    bufw,
		json:   json.NewEncoder(bufw),
	}
}

type Trace struct {
	ticks *atomic.Int64

	mu     sync.Mutex
	closer io.Closer
	out    *bufio.Writer
	json   *json.Encoder
}

func (t *Trace) encodeJSON(ev *tracev) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.json.Encode(ev); err != nil {
		panic(err)
	}
	_, _ = t.out.Write([]byte{','})
}

func (t *Trace) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, _ = t.out.WriteString("]\n")
	t.out.Flush()

	if t.closer != nil {
		t.closer.Close()
	}
}

func (t *Trace) t() int64 {
	if t.ticks == nil {
		return time.Now().UnixMicro()
	}
	return t.ticks.Add(1)
}

func (r *Root) pid() int64 {
	return int64(r.idx)
}

func (r *Root) tid() int64 {
	return int64(r.idx)*1000 + int64(r.shard)
}

func (t *Trace) WithRoot(ctx context.Context, idx boostpb.DomainIndex, shard *uint) context.Context {
	root := &Root{
		parent: t,
		idx:    idx,
		shard:  common.UnwrapOr(shard, 0),
	}

	t.encodeJSON(&tracev{
		Name: "process_name",
		Cat:  "",
		T:    "M",
		Pid:  root.pid(),
		Args: map[string]any{"name": fmt.Sprintf("Domain %d", root.idx)},
	})

	t.encodeJSON(&tracev{
		Name: "thread_name",
		Cat:  "",
		T:    "M",
		Pid:  root.pid(),
		Tid:  root.tid(),
		Args: map[string]any{"name": fmt.Sprintf("Shard %d", root.shard)},
	})

	return context.WithValue(ctx, spanKey{}, root)
}

func WithRoot(ctx context.Context, idx boostpb.DomainIndex, shard *uint) context.Context {
	return DefaultTrace.WithRoot(ctx, idx, shard)
}

type Root struct {
	parent *Trace
	idx    boostpb.DomainIndex
	shard  uint
}

func (r *Root) t() int64 {
	return r.parent.t()
}

func (r *Root) WithSpan(name string) *Span {
	if name != "" {
		r.encodeJSON(&tracev{
			Name: name,
			T:    "B",
			Ts:   r.t(),
		})
	}

	return &Span{
		parent: r,
		name:   name,
	}
}

func (r *Root) encodeJSON(ev *tracev) {
	ev.Pid = r.pid()
	ev.Tid = r.tid()
	r.parent.encodeJSON(ev)
}

type Span struct {
	parent *Root
	name   string
	args   map[string]any
}

func (s *Span) WithSpan(name string) *Span {
	return s.parent.WithSpan(name)
}

func (s *Span) Close() {
	if s.name == "" {
		return
	}

	s.parent.encodeJSON(&tracev{
		Name: s.name,
		Cat:  "",
		T:    "E",
		Ts:   s.parent.t(),
		Args: s.args,
	})
	s.name = ""
}

func (s *Span) CloseWithReason(reason any) {
	closed, err := json.Marshal(reason)
	if err != nil {
		panic(err)
	}
	s.Arg("closed", json.RawMessage(closed)).Close()
	s.Close()
}

func (s *Span) Instant(name string, args map[string]any) {
	for k, v := range args {
		args[k] = convertArg(v)
	}
	s.parent.encodeJSON(&tracev{
		Name: name,
		Cat:  "",
		T:    "i",
		Ts:   s.parent.t(),
		Args: args,
	})
}

func (s *Span) StartFlow(name string, id int32, args map[string]any) {
	for k, v := range args {
		args[k] = convertArg(v)
	}
	s.parent.encodeJSON(&tracev{
		Name:  name,
		Ident: id,
		T:     "s",
		Ts:    s.parent.t(),
		Args:  args,
	})
}

func (s *Span) FinishFlow(name string, id int32) {
	s.parent.encodeJSON(&tracev{
		Name:  name,
		Ident: id,
		T:     "f",
		Ts:    s.parent.t(),
	})
}

type Traceable interface {
	Trace() any
}

func convertArg(value any) any {
	var raw json.RawMessage
	var err error

	switch dbg := value.(type) {
	case Traceable:
		raw, err = json.Marshal(dbg.Trace())
	case []boostpb.Record:
		raw, err = boostpb.DebugRecords(dbg).MarshalJSON()
	case []boostpb.Row:
		raw, err = boostpb.DebugRows(dbg).MarshalJSON()
	}
	if err != nil {
		panic(err)
	}
	if raw == nil {
		return value
	}
	return raw
}

func (s *Span) Arg(name string, value any) *Span {
	if s.args == nil {
		s.args = make(map[string]any)
	}
	s.args[name] = convertArg(value)
	return s
}

type spanKey struct{}

type ispan interface {
	WithSpan(string) *Span
}

func WithSpan(ctx context.Context, name string, m Traceable) (context.Context, *Span) {
	espan := ctx.Value(spanKey{}).(ispan)
	nspan := espan.WithSpan(name)
	if m != nil {
		nspan = nspan.Arg("input", m)
	}
	return context.WithValue(ctx, spanKey{}, nspan), nspan
}

func GetSpan(ctx context.Context) *Span {
	switch span := ctx.Value(spanKey{}).(type) {
	case *Span:
		return span
	case *Root:
		return span.WithSpan("")
	default:
		panic("no Span in context")
	}
}

func HasSpan(ctx context.Context) bool {
	_, ok := ctx.Value(spanKey{}).(*Span)
	return ok
}
