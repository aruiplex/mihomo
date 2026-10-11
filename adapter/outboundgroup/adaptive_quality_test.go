package outboundgroup

import (
	"context"
	"encoding/json"
	C "github.com/metacubex/mihomo/constant"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAdaptiveQualitySurvivesReloadWithoutClosingHealthyConnection(t *testing.T) {
	oldHome := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	defer C.SetHomeDir(oldHome)
	nodeA := &adaptiveTestProxy{name: "A", identity: "config-A", kind: C.Http, dial: adaptiveTestOK}
	nodeB := &adaptiveTestProxy{name: "B", identity: "config-B", kind: C.Http, dial: adaptiveTestOK}
	option := AdaptiveOption{PersistQuality: true}
	first := adaptiveTestGroup(t, option, nodeA, nodeB)
	connection, err := first.DialContext(context.Background(), adaptiveTestMetadata("chat.test"))
	if err != nil {
		t.Fatal(err)
	}
	if err = first.Feedback("chat.test:443", "timeout"); err != nil {
		t.Fatal(err)
	}
	first.Snapshot() // Public observation flushes a checkpoint; background debounce is also bounded.
	reloaded := adaptiveTestGroup(t, option, nodeA, nodeB)
	next, err := reloaded.DialContext(context.Background(), adaptiveTestMetadata("chat.test"))
	if err != nil {
		t.Fatal(err)
	}
	if next.(*adaptiveConn).node != "B" {
		t.Fatal("feedback forgotten after group reload")
	}
	// The old group's later close must not overwrite the new group's evidence.
	_ = connection.Close()
	_ = next.Close()
	reloaded.Snapshot()
	time.Sleep(650 * time.Millisecond)
	restarted := adaptiveTestGroup(t, option, nodeA, nodeB)
	snapshot := restarted.Snapshot().(map[string]any)
	rows := snapshot["records"].([]adaptiveRecord)
	found := false
	for _, row := range rows {
		if row.Node == "B" && row.Established == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("old group overwrote new checkpoint")
	}
	files, _ := filepath.Glob(filepath.Join(C.Path.HomeDir(), "adaptive-quality", "*.json"))
	info, err := os.Stat(files[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("state is not private")
	}
}

func TestAdaptiveQualityIdentityExpiryAndCorruptStateRecovery(t *testing.T) {
	oldHome := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	defer C.SetHomeDir(oldHome)
	node := &adaptiveTestProxy{name: "A", identity: "old-config", kind: C.Http, dial: adaptiveTestOK}
	opt := AdaptiveOption{PersistQuality: true, MaxRecords: 2}
	first := adaptiveTestGroup(t, opt, node)
	for _, host := range []string{"one.test", "two.test", "three.test"} {
		conn, err := first.DialContext(context.Background(), adaptiveTestMetadata(host))
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}
	if len(first.Snapshot().(map[string]any)["records"].([]adaptiveRecord)) != 2 {
		t.Fatal("record capacity not enforced")
	}
	time.Sleep(650 * time.Millisecond)
	files, _ := filepath.Glob(filepath.Join(C.Path.HomeDir(), "adaptive-quality", "*.json"))
	file := files[0]
	data, _ := os.ReadFile(file)
	// Same display name with changed credentials/configuration must be cold.
	changed := &adaptiveTestProxy{name: "A", identity: "new-config", kind: C.Http, dial: adaptiveTestOK}
	fresh := adaptiveTestGroup(t, opt, changed)
	if len(fresh.Snapshot().(map[string]any)["records"].([]adaptiveRecord)) != 0 {
		t.Fatal("node identity change retained evidence")
	}
	var stored adaptiveQualityFile
	_ = json.Unmarshal(data, &stored)
	for i := range stored.Records {
		stored.Records[i].Touched = time.Now().Add(-25 * time.Hour)
	}
	expired, _ := json.Marshal(stored)
	_ = os.WriteFile(file, expired, 0600)
	old := adaptiveTestGroup(t, opt, node)
	if len(old.Snapshot().(map[string]any)["records"].([]adaptiveRecord)) != 0 {
		t.Fatal("expired evidence restored")
	}
	_ = os.WriteFile(file, []byte("invalid json"), 0600)
	recovered := adaptiveTestGroup(t, opt, node)
	if len(recovered.Snapshot().(map[string]any)["records"].([]adaptiveRecord)) != 0 {
		t.Fatal("corrupt state did not cold-start")
	}
}
