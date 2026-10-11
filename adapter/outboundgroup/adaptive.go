package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/sing/common/buf"
)

// Adaptive is an experimental TCP establishment policy. It never migrates or
// closes another connection. Each candidate gets one attempt before payload.
// UDP and handshakes that cannot finish without payload remain unsupported.
type Adaptive struct {
	*GroupBase
	opt            AdaptiveOption
	mu             sync.Mutex
	records        map[string]*adaptiveRecord
	events         []adaptiveEvent
	now            func() time.Time
	sequence       uint64
	qualityPath    string
	qualityTimer   *time.Timer
	nodeIdentities map[string]string
	qualityError   string
}

type AdaptiveOption struct {
	PersistQuality  bool              `group:"quality-persistence,omitempty"`
	QualityTTL      int               `group:"quality-ttl,omitempty"` // seconds
	MaxAttempts     int               `group:"max-attempts,omitempty"`
	DialBudget      int               `group:"dial-budget,omitempty"` // milliseconds, whole establishment
	AttemptTimeout  int               `group:"attempt-timeout,omitempty"`
	Cooldown        int               `group:"failure-cooldown,omitempty"` // milliseconds
	MaxRecords      int               `group:"max-records,omitempty"`
	PreferredRegion string            `group:"preferred-region,omitempty"`
	Regions         map[string]string `group:"regions,omitempty"`
}

type adaptiveRecord struct {
	Target          string    `json:"target"`
	Node            string    `json:"node"`
	Until           time.Time `json:"cooldownUntil"`
	Touched         time.Time `json:"touched"`
	Dials           uint64    `json:"dials"`
	Failures        uint64    `json:"failures"`
	Established     uint64    `json:"established"`
	Selection       uint64    `json:"selection"`
	LastEstablished time.Time `json:"lastEstablished"`
	Closed          uint64    `json:"closed"`
	Reason          string    `json:"reason"`
}

type adaptiveEvent struct {
	At           time.Time `json:"at"`
	Target       string    `json:"target"`
	Node         string    `json:"node"`
	Kind         string    `json:"kind"`
	Reason       string    `json:"reason,omitempty"`
	DurationMS   int64     `json:"durationMs,omitempty"`
	ReadBytes    uint64    `json:"readBytes,omitempty"`
	WrittenBytes uint64    `json:"writtenBytes,omitempty"`
}

// Retryable prevents the enclosing tunnel's retry loop from multiplying the
// group's attempt budget or replaying a partially sent lazy handshake.
type adaptiveExhausted struct{ error }

func (adaptiveExhausted) Retryable() bool { return false }
func (e adaptiveExhausted) Unwrap() error { return e.error }

func NewAdaptive(common GroupCommonOption, opt AdaptiveOption, empty C.Proxy, providers []P.ProxyProvider) (*Adaptive, error) {
	if opt.MaxAttempts == 0 {
		opt.MaxAttempts = 3
	}
	if opt.DialBudget == 0 {
		opt.DialBudget = 6000
	}
	if opt.AttemptTimeout == 0 {
		opt.AttemptTimeout = 2000
	}
	if opt.Cooldown == 0 {
		opt.Cooldown = 30000
	}
	if opt.MaxRecords == 0 {
		opt.MaxRecords = 4096
	}
	if opt.MaxAttempts < 1 || opt.MaxAttempts > 16 || opt.DialBudget < 1 || opt.DialBudget > 30000 || opt.AttemptTimeout < 1 || opt.AttemptTimeout > opt.DialBudget || opt.Cooldown < 1 || opt.Cooldown > 3600000 || opt.MaxRecords < 1 || opt.MaxRecords > 65536 {
		return nil, errors.New("adaptive: invalid attempt, timeout, cooldown or record limit")
	}
	if opt.QualityTTL == 0 {
		opt.QualityTTL = 86400
	}
	if opt.QualityTTL < 60 || opt.QualityTTL > 604800 {
		return nil, errors.New("adaptive: invalid quality TTL")
	}
	regions := make(map[string]string, len(opt.Regions))
	for node, region := range opt.Regions {
		regions[node] = region
	}
	opt.Regions = regions
	a := &Adaptive{GroupBase: NewGroupBase(GroupBaseOption{
		Name: common.Name, Type: C.Adaptive, Hidden: common.Hidden, Icon: common.Icon,
		Filter: common.Filter, ExcludeFilter: common.ExcludeFilter, ExcludeType: common.ExcludeType,
		EmptyFallback: empty, Providers: providers,
	}), opt: opt, records: make(map[string]*adaptiveRecord), now: time.Now}
	a.initQuality()
	return a, nil
}

func adaptiveTarget(ctx context.Context, m *C.Metadata) string {
	if target, ok := ctx.Value(C.RoutingTargetKey{}).(string); ok {
		return target
	}
	return strings.ToLower(m.RemoteAddress())
}

func (a *Adaptive) record(target, node string) *adaptiveRecord {
	key := target + "\x00" + node
	if r := a.records[key]; r != nil && a.now().Sub(r.Touched) <= time.Duration(a.opt.QualityTTL)*time.Second {
		r.Touched = a.now()
		return r
	}
	delete(a.records, key) // expired instance of this key must not evict another live record
	if len(a.records) >= a.opt.MaxRecords {
		var oldestKey string
		var oldest time.Time
		for k, r := range a.records {
			if oldestKey == "" || r.Touched.Before(oldest) {
				oldestKey, oldest = k, r.Touched
			}
		}
		delete(a.records, oldestKey)
	}
	r := &adaptiveRecord{Target: target, Node: node, Touched: a.now()}
	a.records[key] = r
	return r
}

func (a *Adaptive) event(e adaptiveEvent) {
	e.At = a.now()
	if len(a.events) == 256 {
		copy(a.events, a.events[1:])
		a.events = a.events[:255]
	}
	a.events = append(a.events, e)
	a.scheduleQuality()
}

// Global failure evidence below is deduplicated by host, not port.
func (a *Adaptive) candidates(target string, proxies []C.Proxy) []C.Proxy {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.syncQualityNodes(proxies)
	now := a.now()
	preferred := a.opt.PreferredRegion
	// Preserve the previous successful region; failures/feedback change future
	// establishments only. Latency does not enter this prototype's ordering.
	var latest uint64
	for _, r := range a.records {
		if r.Target == target && r.Selection > latest {
			if region := a.opt.Regions[r.Node]; region != "" {
				preferred, latest = region, r.Selection
			}
		}
	}
	if preferred == "" && len(proxies) > 0 {
		preferred = a.opt.Regions[proxies[0].Name()]
	}
	choices := make([]C.Proxy, 0, len(proxies))
	seen := map[string]bool{}
	for _, p := range proxies {
		if seen[p.Name()] {
			continue
		}
		seen[p.Name()] = true
		r := a.records[target+"\x00"+p.Name()]
		if r == nil || !r.Until.After(now) {
			choices = append(choices, p)
		}
	}
	failedHosts := map[string]map[string]bool{}
	for _, r := range a.records {
		if !r.Until.After(now) {
			continue
		}
		host, _, err := net.SplitHostPort(r.Target)
		if err != nil {
			continue
		}
		if failedHosts[r.Node] == nil {
			failedHosts[r.Node] = map[string]bool{}
		}
		failedHosts[r.Node][host] = true
	}
	scores := map[string]int{}
	for _, p := range choices {
		score := 0
		if len(failedHosts[p.Name()]) >= 3 {
			score += 4
		}
		if !p.AliveForTestUrl("") {
			score += 2
		}
		if preferred != "" && a.opt.Regions[p.Name()] != preferred {
			score++
		}
		scores[p.Name()] = score
	}
	sort.SliceStable(choices, func(i, j int) bool { return scores[choices[i].Name()] < scores[choices[j].Name()] })
	return choices
}

func adaptiveProtected(m *C.Metadata) bool {
	ip := m.DstIP.Unmap()
	// Explicit DIRECT rules bypass this group entirely. For a DIRECT-first pool,
	// numeric/private destinations must never fall through to a proxy candidate.
	if !ip.IsValid() {
		ip, _ = netip.ParseAddr(m.Host)
		ip = ip.Unmap()
	}
	host := strings.TrimSuffix(strings.ToLower(m.Host), ".")
	return (ip.IsValid() && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified())) || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local")
}

func (a *Adaptive) DialContext(ctx context.Context, m *C.Metadata) (C.Conn, error) {
	target := adaptiveTarget(ctx, m)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(a.opt.DialBudget)*time.Millisecond)
	defer cancel()
	proxies := a.GetProxies(true)
	if len(proxies) > 0 && proxies[0].Type() == C.Direct {
		protected := adaptiveProtected(m)
		if !protected && !m.DstIP.IsValid() && m.Host != "" {
			// An unresolved or mixed public/private answer must not leak LAN traffic.
			// Resolve within the same total dial budget. Unknown stays DIRECT-only.
			ips, err := resolver.LookupIP(ctx, m.Host)
			protected = err != nil || len(ips) == 0
			for _, ip := range ips {
				resolved := *m
				resolved.DstIP = ip
				if adaptiveProtected(&resolved) {
					protected = true
					break
				}
			}
		}
		if protected {
			proxies = proxies[:1]
		}
	}
	candidates := a.candidates(target, proxies)
	var last error = errors.New("all candidates cooling down")
	for i, p := range candidates {
		if i >= a.opt.MaxAttempts || ctx.Err() != nil {
			break
		}
		attempt, stop := context.WithTimeout(ctx, time.Duration(a.opt.AttemptTimeout)*time.Millisecond)
		a.mu.Lock()
		a.record(target, p.Name()).Dials++
		a.mu.Unlock()
		conn, err := p.DialContext(attempt, m)
		phase := "dial-error"
		// With a dial error the adapter owns cleanup and the returned connection
		// is undefined (some adapters wrap a nil transport). Do not use it.
		if err != nil {
			conn = nil
		}
		if err == nil {
			err = attempt.Err()
		}
		if err == nil && conn == nil {
			err = errors.New("adapter returned a nil connection")
		}
		if err == nil && N.NeedHandshake(conn) {
			phase = "handshake-error"
			err = primeAdaptiveHandshake(attempt, conn)
		}
		stop()
		if errors.Is(err, errAdaptiveNeedsPayload) {
			_ = conn.Close()
			last = err
			a.mu.Lock()
			a.event(adaptiveEvent{Target: target, Node: p.Name(), Kind: "unsupported", Reason: "handshake-needs-payload"})
			a.mu.Unlock()
			continue
		}
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			last = err
			// Caller cancellation is not evidence against the route.
			if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				break
			}
			a.mu.Lock()
			r := a.record(target, p.Name())
			r.Failures++
			r.Reason = phase
			r.Until = a.now().Add(time.Duration(a.opt.Cooldown) * time.Millisecond)
			a.event(adaptiveEvent{Target: target, Node: p.Name(), Kind: "dial-failed", Reason: r.Reason})
			a.mu.Unlock()
			continue
		}
		conn.AppendToChains(a)
		a.mu.Lock()
		r := a.record(target, p.Name())
		r.Established++
		r.LastEstablished = a.now()
		a.sequence++
		r.Selection = a.sequence
		r.Until = time.Time{}
		r.Reason = ""
		a.event(adaptiveEvent{Target: target, Node: p.Name(), Kind: "established"})
		a.mu.Unlock()
		return &adaptiveConn{Conn: conn, group: a, target: target, node: p.Name(), started: a.now()}, nil
	}
	if ctx.Err() != nil {
		last = ctx.Err()
	}
	return nil, adaptiveExhausted{fmt.Errorf("adaptive establishment exhausted: %w", last)}
}

// An adapter has received destination metadata only, so an empty write can
// transmit protocol headers but never application bytes. This happens before
// handing the connection to the tunnel, inside the existing attempt budget.
var errAdaptiveNeedsPayload = errors.New("adaptive: handshake cannot finish without payload")

func primeAdaptiveHandshake(ctx context.Context, conn C.Conn) (err error) {
	done := N.SetupContextForConn(ctx, conn)
	defer done(&err)
	if err = ctx.Err(); err != nil {
		return
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err = conn.SetDeadline(deadline); err != nil {
			return
		}
	}
	n, writeErr := conn.Write(nil)
	if writeErr != nil {
		return writeErr
	}
	if n != 0 {
		return errors.New("adapter returned bytes for an empty handshake write")
	}
	if N.NeedHandshake(conn) {
		return errAdaptiveNeedsPayload
	}
	if err = ctx.Err(); err != nil {
		return
	}
	// Remove the temporary handshake deadline before business traffic begins.
	return conn.SetDeadline(time.Time{})
}

// Feedback is scoped to the actual last established node for this target.
// A later successful close cannot erase newer user feedback.
func (a *Adaptive) Feedback(target, reason string) error {
	proxies := a.GetProxies(false)
	switch reason {
	case "unknown", "timeout", "disconnect", "unusable":
	default:
		return errors.New("unsupported feedback reason")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.syncQualityNodes(proxies)
	var node string
	var latest uint64
	for _, r := range a.records {
		if r.Target == target && r.Selection > latest {
			node, latest = r.Node, r.Selection
		}
	}
	if node == "" {
		return errors.New("target has no recent established route")
	}
	r := a.record(target, node)
	r.Failures++
	r.Reason = "feedback:" + reason
	r.Until = a.now().Add(time.Duration(a.opt.Cooldown) * time.Millisecond)
	a.event(adaptiveEvent{Target: target, Node: node, Kind: "feedback", Reason: reason})
	return nil
}

func (a *Adaptive) Snapshot() any {
	proxies := a.GetProxies(false)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.syncQualityNodes(proxies)
	if a.opt.PersistQuality {
		a.saveQuality()
	}
	records := make([]adaptiveRecord, 0, len(a.records))
	for _, r := range a.records {
		records = append(records, *r)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Target == records[j].Target {
			return records[i].Node < records[j].Node
		}
		return records[i].Target < records[j].Target
	})
	return map[string]any{"records": records, "events": append([]adaptiveEvent(nil), a.events...), "scope": "TCP transport only; application outcome unknown; local expiring history", "persistence": a.opt.PersistQuality, "persistenceError": a.qualityError, "ttlSeconds": a.opt.QualityTTL}
}

func (a *Adaptive) Now() string                      { return "target-dependent" }
func (a *Adaptive) Proxies() []C.Proxy               { return a.GetProxies(false) }
func (a *Adaptive) Providers() []P.ProxyProvider     { return a.providers }
func (a *Adaptive) Unwrap(*C.Metadata, bool) C.Proxy { return nil } // do not bypass target selection
func (a *Adaptive) SupportUDP() bool                 { return false }
func (a *Adaptive) IsL3Protocol(*C.Metadata) bool    { return false }
func (a *Adaptive) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, p := range a.GetProxies(false) {
		all = append(all, p.Name())
	}
	return json.Marshal(map[string]any{"type": a.Type().String(), "now": a.Now(), "all": all, "experimental": true, "hidden": a.Hidden(), "icon": a.Icon()})
}

// Deliberately do not expose Upstream/ReadWaiter: optimized copy must still cross
// these methods so transport errors and byte counts remain observable.
type adaptiveConn struct {
	C.Conn
	group         *Adaptive
	target, node  string
	started       time.Time
	mu            sync.Mutex
	closed        bool
	read, written uint64
	outcome       string
}

func (c *adaptiveConn) observe(n int, err error, read bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if read {
		c.read += uint64(n)
	} else {
		c.written += uint64(n)
	}
	if err == nil || c.closed {
		return
	}
	if errors.Is(err, io.EOF) {
		if c.outcome == "" {
			c.outcome = "transport-eof"
		}
		return
	}
	// A deadline installed by the caller, local Close or explicit cancellation
	// cannot establish that a node is bad. Record without applying a penalty.
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		if c.outcome == "" {
			c.outcome = "local-close"
		}
		return
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		if c.outcome == "" {
			c.outcome = "transport-timeout-unknown"
		}
		return
	}
	if c.outcome == "transport-error" {
		return
	}
	c.outcome = "transport-error"
	c.group.mu.Lock()
	r := c.group.record(c.target, c.node)
	r.Failures++
	r.Reason = "transport-error"
	r.Until = c.group.now().Add(time.Duration(c.group.opt.Cooldown) * time.Millisecond)
	c.group.event(adaptiveEvent{Target: c.target, Node: c.node, Kind: "transport-error", Reason: "unknown-root-cause"})
	c.group.mu.Unlock()
}
func (c *adaptiveConn) Read(p []byte) (int, error) {
	n, e := c.Conn.Read(p)
	c.observe(n, e, true)
	return n, e
}
func (c *adaptiveConn) Write(p []byte) (int, error) {
	n, e := c.Conn.Write(p)
	if e == nil && n != len(p) {
		e = io.ErrShortWrite
	}
	c.observe(n, e, false)
	return n, e
}
func (c *adaptiveConn) ReadBuffer(b *buf.Buffer) error { _, e := b.ReadOnceFrom(c); return e }
func (c *adaptiveConn) WriteBuffer(b *buf.Buffer) error {
	defer b.Release()
	_, e := c.Write(b.Bytes())
	return e
}
func (c *adaptiveConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	outcome := c.outcome
	if outcome == "" {
		outcome = "local-close-unknown"
	}
	e := adaptiveEvent{Target: c.target, Node: c.node, Kind: "closed", Reason: outcome, DurationMS: c.group.now().Sub(c.started).Milliseconds(), ReadBytes: c.read, WrittenBytes: c.written}
	c.mu.Unlock()
	err := c.Conn.Close()
	c.group.mu.Lock()
	c.group.record(c.target, c.node).Closed++
	c.group.event(e)
	c.group.mu.Unlock()
	return err
}
