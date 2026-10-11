package outboundgroup

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type adaptiveTestProxy struct {
	C.Proxy
	name string
	kind C.AdapterType
	dial func(context.Context, *C.Metadata) (C.Conn, error)
}

func (p *adaptiveTestProxy) Name() string                { return p.name }
func (p *adaptiveTestProxy) Type() C.AdapterType         { return p.kind }
func (p *adaptiveTestProxy) AliveForTestUrl(string) bool { return true }
func (p *adaptiveTestProxy) DialContext(ctx context.Context, m *C.Metadata) (C.Conn, error) {
	return p.dial(ctx, m)
}

type adaptiveTestConn struct {
	C.Conn
	mu             sync.Mutex
	writes, closes int
	lazy           bool
	primeable      bool
	writeSizes     []int
	writeError     error
	readError      error
}

func (c *adaptiveTestConn) NeedHandshake() bool      { return c.lazy }
func (c *adaptiveTestConn) Read([]byte) (int, error) { return 0, c.readError }
func (c *adaptiveTestConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	c.writeSizes = append(c.writeSizes, len(p))
	if c.writeError != nil {
		return 0, c.writeError
	}
	if c.primeable && len(p) == 0 {
		c.lazy = false
	}
	return len(p), nil
}
func (c *adaptiveTestConn) Close() error                  { c.mu.Lock(); defer c.mu.Unlock(); c.closes++; return nil }
func (c *adaptiveTestConn) AppendToChains(C.ProxyAdapter) {}

func adaptiveTestGroup(t *testing.T, opt AdaptiveOption, ps ...C.Proxy) *Adaptive {
	t.Helper()
	hc := provider.NewHealthCheck(ps, "", 100, 0, true, nil)
	pd, err := provider.NewCompatibleProvider("fixture", ps, hc)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewAdaptive(GroupCommonOption{Name: "fixture"}, opt, ps[0], []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func adaptiveTestMetadata(host string) *C.Metadata {
	return &C.Metadata{NetWork: C.TCP, Host: host, DstPort: 443}
}
func adaptiveTestNode(name string, dial func(context.Context, *C.Metadata) (C.Conn, error)) C.Proxy {
	return &adaptiveTestProxy{name: name, kind: C.Http, dial: dial}
}
func adaptiveTestOK(context.Context, *C.Metadata) (C.Conn, error) { return &adaptiveTestConn{}, nil }
func adaptiveTestFail(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, errors.New("injected dial failure")
}

func TestAdaptiveTargetRecoveryRegionAndCooldown(t *testing.T) {
	var attempts []string
	bad := true
	nodes := []C.Proxy{}
	for _, name := range []string{"A", "C", "B"} {
		node := name
		nodes = append(nodes, adaptiveTestNode(node, func(ctx context.Context, m *C.Metadata) (C.Conn, error) {
			attempts = append(attempts, node)
			if bad && (node == "A" || node == "B") {
				return adaptiveTestFail(ctx, m)
			}
			return adaptiveTestOK(ctx, m)
		}))
	}
	group := adaptiveTestGroup(t, AdaptiveOption{PreferredRegion: "SG", Regions: map[string]string{"A": "SG", "B": "SG", "C": "US"}}, nodes...)
	now := time.Now()
	group.now = func() time.Time { return now }
	c, err := group.DialContext(context.Background(), adaptiveTestMetadata("chat.test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !reflect.DeepEqual(attempts, []string{"A", "B", "C"}) {
		t.Fatalf("same-region ordering: %v", attempts)
	}
	attempts = nil
	other, err := group.DialContext(context.Background(), adaptiveTestMetadata("other.test"))
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
	if attempts[0] != "A" {
		t.Fatalf("single target contaminated other target: %v", attempts)
	}
	// Feedback temporarily excludes C; an expired failure must become eligible.
	if err := group.Feedback("chat.test:443", "unknown"); err != nil {
		t.Fatal(err)
	}
	if _, err := group.DialContext(context.Background(), adaptiveTestMetadata("chat.test")); err == nil {
		t.Fatal("cooling candidates should not be retried")
	}
	bad = false
	now = now.Add(31 * time.Second)
	attempts = nil
	recovered, err := group.DialContext(context.Background(), adaptiveTestMetadata("chat.test"))
	if err != nil {
		t.Fatal(err)
	}
	recovered.Close()
	if len(attempts) != 1 {
		t.Fatalf("recovery should use one eligible candidate: %v", attempts)
	}
}

func TestAdaptiveAttemptAndTimeBudgets(t *testing.T) {
	calls := 0
	fail := func(ctx context.Context, m *C.Metadata) (C.Conn, error) { calls++; return adaptiveTestFail(ctx, m) }
	g := adaptiveTestGroup(t, AdaptiveOption{MaxAttempts: 2}, adaptiveTestNode("A", fail), adaptiveTestNode("B", fail), adaptiveTestNode("C", fail))
	_, err := g.DialContext(context.Background(), adaptiveTestMetadata("bounded.test"))
	var stop interface{ Retryable() bool }
	if calls != 2 || !errors.As(err, &stop) || stop.Retryable() {
		t.Fatalf("budget or enclosing retry guard: calls=%d err=%v", calls, err)
	}
	calls = 0
	slow := func(ctx context.Context, m *C.Metadata) (C.Conn, error) { calls++; <-ctx.Done(); return nil, ctx.Err() }
	g = adaptiveTestGroup(t, AdaptiveOption{DialBudget: 75, AttemptTimeout: 50}, adaptiveTestNode("A", slow), adaptiveTestNode("B", slow), adaptiveTestNode("C", slow))
	start := time.Now()
	_, err = g.DialContext(context.Background(), adaptiveTestMetadata("slow.test"))
	if calls != 2 || err == nil || time.Since(start) > 300*time.Millisecond {
		t.Fatalf("whole establishment budget: calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
	}
}

func TestAdaptiveFeedbackPreservesConnectionsAndRecordsLifecycle(t *testing.T) {
	first := &adaptiveTestConn{readError: io.EOF}
	second := &adaptiveTestConn{}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) { return first, nil }), adaptiveTestNode("B", func(context.Context, *C.Metadata) (C.Conn, error) { return second, nil }))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("feedback.test"))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Feedback("feedback.test:443", "timeout"); err != nil {
		t.Fatal(err)
	}
	next, err := g.DialContext(context.Background(), adaptiveTestMetadata("feedback.test"))
	if err != nil {
		t.Fatal(err)
	}
	if first.closes != 0 {
		t.Fatal("feedback closed an established connection")
	}
	c.Write([]byte("payload"))
	c.Read(make([]byte, 1))
	c.Close()
	c.Close()
	next.Close()
	if first.closes != 1 {
		t.Fatalf("underlying close count %d", first.closes)
	}
	snapshot := g.Snapshot().(map[string]any)
	records := snapshot["records"].([]adaptiveRecord)
	if records[0].Failures != 1 || records[0].Reason != "feedback:timeout" || records[0].Closed != 1 {
		t.Fatalf("close overwrote feedback or counted normal EOF as failure: %+v", records)
	}
	for _, e := range snapshot["events"].([]adaptiveEvent) {
		if e.Kind == "closed" && e.Node == "A" && (e.Reason != "transport-eof" || e.WrittenBytes != 7) {
			t.Fatalf("bad lifecycle observation %+v", e)
		}
	}
	if err := g.Feedback("unseen.test:443", "unknown"); err == nil {
		t.Fatal("unobserved feedback accepted")
	}
}

func TestAdaptiveDoesNotWriteLazyHandshakePayload(t *testing.T) {
	lazy := &adaptiveTestConn{lazy: true}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("lazy", func(context.Context, *C.Metadata) (C.Conn, error) { return lazy, nil }), adaptiveTestNode("eager", adaptiveTestOK))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("lazy.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if lazy.writes != 1 || len(lazy.writeSizes) != 1 || lazy.writeSizes[0] != 0 || lazy.closes != 1 {
		t.Fatalf("unsafe handshake: writes=%d closes=%d", lazy.writes, lazy.closes)
	}
	for _, r := range g.Snapshot().(map[string]any)["records"].([]adaptiveRecord) {
		if r.Node == "lazy" && r.Failures != 0 {
			t.Fatal("unsupported protocol classified as route failure")
		}
	}
}

func TestAdaptiveIndependentHostEvidence(t *testing.T) {
	var choices []string
	a := adaptiveTestNode("A", func(ctx context.Context, m *C.Metadata) (C.Conn, error) {
		choices = append(choices, "A")
		if m.Host != "new.test" {
			return adaptiveTestFail(ctx, m)
		}
		return adaptiveTestOK(ctx, m)
	})
	b := adaptiveTestNode("B", func(ctx context.Context, m *C.Metadata) (C.Conn, error) {
		choices = append(choices, "B")
		return adaptiveTestOK(ctx, m)
	})
	g := adaptiveTestGroup(t, AdaptiveOption{}, a, b)
	now := time.Now()
	g.now = func() time.Time { return now }
	for _, port := range []uint16{80, 443, 8443} {
		m := adaptiveTestMetadata("one.test")
		m.DstPort = port
		c, err := g.DialContext(context.Background(), m)
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	choices = nil
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("new.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if choices[0] != "A" {
		t.Fatal("three ports on one host counted as independent targets")
	}
	for _, host := range []string{"two.test", "three.test"} {
		c, err := g.DialContext(context.Background(), adaptiveTestMetadata(host))
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	choices = nil
	c, err = g.DialContext(context.Background(), adaptiveTestMetadata("new.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if choices[0] != "B" {
		t.Fatal("independent failures did not downgrade node")
	}
	now = now.Add(31 * time.Second)
	choices = nil
	c, err = g.DialContext(context.Background(), adaptiveTestMetadata("new.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if choices[0] != "A" {
		t.Fatal("global evidence did not expire")
	}
}

func TestAdaptiveDirectConstraintsAndOriginalTarget(t *testing.T) {
	calls := []string{}
	direct := &adaptiveTestProxy{name: "DIRECT", kind: C.Direct, dial: func(ctx context.Context, m *C.Metadata) (C.Conn, error) {
		calls = append(calls, "DIRECT")
		return adaptiveTestFail(ctx, m)
	}}
	proxy := adaptiveTestNode("proxy", func(ctx context.Context, m *C.Metadata) (C.Conn, error) {
		calls = append(calls, "proxy")
		return adaptiveTestOK(ctx, m)
	})
	g := adaptiveTestGroup(t, AdaptiveOption{}, direct, proxy)
	m := adaptiveTestMetadata("private.test")
	m.DstIP = netip.MustParseAddr("10.0.0.1")
	_, err := g.DialContext(context.Background(), m)
	if err == nil || !reflect.DeepEqual(calls, []string{"DIRECT"}) {
		t.Fatalf("private destination leaked to proxy: %v %v", calls, err)
	}
	calls = nil
	m = adaptiveTestMetadata("")
	m.DstIP = netip.MustParseAddr("203.0.113.1")
	ctx := context.WithValue(context.Background(), C.RoutingTargetKey{}, "original.test:443")
	c, err := g.DialContext(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if !reflect.DeepEqual(calls, []string{"DIRECT", "proxy"}) {
		t.Fatalf("public direct failure did not use proxy: %v", calls)
	}
	if err := g.Feedback("original.test:443", "unknown"); err != nil {
		t.Fatalf("original domain lost: %v", err)
	}
}

func TestAdaptiveCallerCancellationAndLocalDeadlineAreNotNodeFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", adaptiveTestFail))
	_, err := g.DialContext(ctx, adaptiveTestMetadata("cancel.test"))
	if err == nil {
		t.Fatal("cancelled dial succeeded")
	}
	if len(g.Snapshot().(map[string]any)["records"].([]adaptiveRecord)) != 0 {
		t.Fatal("cancelled caller penalized node")
	}
	localTimeout := &net.DNSError{IsTimeout: true}
	raw := &adaptiveTestConn{readError: localTimeout}
	g = adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("deadline.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Read(make([]byte, 1))
	c.Close()
	if g.Snapshot().(map[string]any)["records"].([]adaptiveRecord)[0].Failures != 0 {
		t.Fatal("caller read deadline classified as bad node")
	}
}

func TestAdaptiveBoundedStateAndConcurrentUse(t *testing.T) {
	// The transport is a no-op; concurrency exercises shared policy and feedback.
	g := adaptiveTestGroup(t, AdaptiveOption{MaxRecords: 8}, adaptiveTestNode("A", adaptiveTestOK), adaptiveTestNode("B", adaptiveTestOK))
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				m := adaptiveTestMetadata(string(rune('a'+i)) + ".test")
				c, e := g.DialContext(context.Background(), m)
				if e == nil {
					c.Close()
				}
				g.Snapshot()
			}
		}(i)
	}
	wg.Wait()
	snapshot := g.Snapshot().(map[string]any)
	if len(snapshot["records"].([]adaptiveRecord)) > 8 || len(snapshot["events"].([]adaptiveEvent)) > 256 {
		t.Fatal("unbounded quality state")
	}
}

func TestAdaptiveTransportResetChangesFutureConnectionsOnly(t *testing.T) {
	raw := &adaptiveTestConn{readError: syscall.ECONNRESET}
	choices := []string{}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) { choices = append(choices, "A"); return raw, nil }), adaptiveTestNode("B", func(context.Context, *C.Metadata) (C.Conn, error) {
		choices = append(choices, "B")
		return &adaptiveTestConn{}, nil
	}))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("reset.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Read(make([]byte, 1))
	choices = nil
	next, err := g.DialContext(context.Background(), adaptiveTestMetadata("reset.test"))
	if err != nil {
		t.Fatal(err)
	}
	if len(choices) != 1 || choices[0] != "B" || raw.closes != 0 {
		t.Fatalf("reset handling affected current stream or failed to learn: %v %d", choices, raw.closes)
	}
	c.Close()
	next.Close()
}

func TestAdaptiveFeedbackSurvivesEventRotation(t *testing.T) {
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", adaptiveTestOK), adaptiveTestNode("B", adaptiveTestOK))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("older.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	for i := 0; i < 150; i++ {
		c, err := g.DialContext(context.Background(), adaptiveTestMetadata("busy.test"))
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	if err := g.Feedback("older.test:443", "unknown"); err != nil {
		t.Fatalf("feedback incorrectly depended on finite event log: %v", err)
	}
}

func TestAdaptivePrimesLazyHandshakeBeforePayload(t *testing.T) {
	raw := &adaptiveTestConn{lazy: true, primeable: true}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("lazy", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("lazy.test"))
	if err != nil {
		t.Fatal(err)
	}
	if raw.lazy || len(raw.writeSizes) != 1 || raw.writeSizes[0] != 0 {
		t.Fatalf("handshake was not primed without payload: %+v", raw.writeSizes)
	}
	c.Write([]byte("once"))
	c.Close()
	if !reflect.DeepEqual(raw.writeSizes, []int{0, 4}) {
		t.Fatalf("business payload replayed or put into handshake: %v", raw.writeSizes)
	}
}

func (c *adaptiveTestConn) SetDeadline(time.Time) error { return nil }

func TestAdaptiveEmptyHandshakeFailureUsesBackup(t *testing.T) {
	raw := &adaptiveTestConn{lazy: true, writeError: syscall.ECONNRESET}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }), adaptiveTestNode("B", adaptiveTestOK))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("handshake.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if !reflect.DeepEqual(raw.writeSizes, []int{0}) {
		t.Fatalf("sent payload to failed handshake: %v", raw.writeSizes)
	}
	record := g.Snapshot().(map[string]any)["records"].([]adaptiveRecord)[0]
	if record.Node != "A" || record.Failures != 1 || record.Reason != "handshake-error" {
		t.Fatalf("missing handshake failure evidence: %+v", record)
	}
}

type adaptiveBlockingPrime struct {
	adaptiveTestConn
	stopped chan struct{}
	once    sync.Once
}

func (c *adaptiveBlockingPrime) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writeSizes = append(c.writeSizes, len(p))
	c.mu.Unlock()
	<-c.stopped
	return 0, net.ErrClosed
}
func (c *adaptiveBlockingPrime) Close() error {
	c.once.Do(func() { close(c.stopped) })
	return c.adaptiveTestConn.Close()
}

func TestAdaptiveHandshakeStallHonorsAttemptBudget(t *testing.T) {
	raw := &adaptiveBlockingPrime{adaptiveTestConn: adaptiveTestConn{lazy: true}, stopped: make(chan struct{})}
	g := adaptiveTestGroup(t, AdaptiveOption{DialBudget: 200, AttemptTimeout: 40}, adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) { return raw, nil }), adaptiveTestNode("B", adaptiveTestOK))
	start := time.Now()
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("stalled-handshake.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if time.Since(start) > 180*time.Millisecond || !reflect.DeepEqual(raw.writeSizes, []int{0}) {
		t.Fatal("handshake escaped attempt budget or sent business payload")
	}
}

func TestAdaptiveDoesNotCloseUndefinedDialErrorConnection(t *testing.T) {
	// Some real adapters return a wrapper around nil when their handshake fails.
	// With a dial error, the connection is not usable and cleanup is adapter-owned.
	undefined := &adaptiveTestConn{}
	g := adaptiveTestGroup(t, AdaptiveOption{}, adaptiveTestNode("A", func(context.Context, *C.Metadata) (C.Conn, error) { return undefined, errors.New("dial failed") }), adaptiveTestNode("B", adaptiveTestOK))
	c, err := g.DialContext(context.Background(), adaptiveTestMetadata("undefined.test"))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if undefined.closes != 0 {
		t.Fatal("used undefined connection returned with dial error")
	}
}
