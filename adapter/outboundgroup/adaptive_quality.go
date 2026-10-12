package outboundgroup

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// A replaced group must not overwrite the new group's state when an old,
// healthy connection later closes. Timers have bounded lifetimes (500 ms).
var qualityOwners = struct {
	sync.Mutex
	owners map[string]uint64
	next   uint64
}{owners: make(map[string]uint64)}

type adaptiveQualityFile struct {
	Schema  int               `json:"schema"`
	Policy  string            `json:"policy"`
	Nodes   map[string]string `json:"nodes"`
	Records []adaptiveRecord  `json:"records"`
}

func (a *Adaptive) qualityPolicy() string {
	data, _ := json.Marshal(a.opt)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func (a *Adaptive) initQuality() {
	a.nodeIdentities = make(map[string]string)
	group := fmt.Sprintf("%x", sha256.Sum256([]byte(a.Name())))
	a.qualityPath = filepath.Join(C.Path.HomeDir(), "adaptive-quality", group+".json")
	qualityOwners.Lock()
	defer qualityOwners.Unlock()
	if len(qualityOwners.owners) >= 128 {
		for path := range qualityOwners.owners {
			if path != a.qualityPath {
				delete(qualityOwners.owners, path)
				break
			}
		}
	}
	qualityOwners.next++
	a.qualityOwner = qualityOwners.next
	qualityOwners.owners[a.qualityPath] = a.qualityOwner
	if !a.opt.PersistQuality {
		return
	}
	file, err := os.Open(a.qualityPath)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		a.qualityError = "read-failed"
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 64*1024*1024+1))
	var stored adaptiveQualityFile
	if err != nil || len(data) > 64*1024*1024 || json.Unmarshal(data, &stored) != nil || stored.Schema != 1 || len(stored.Records) > a.opt.MaxRecords {
		a.qualityError = "invalid-state-discarded"
		// Keep at most two quarantined copies; never fail proxy startup on corrupt history.
		_ = os.Rename(a.qualityPath, a.qualityPath+fmt.Sprintf(".corrupt-%d", time.Now().UnixNano()))
		matches, _ := filepath.Glob(a.qualityPath + ".corrupt-*")
		for len(matches) > 2 {
			_ = os.Remove(matches[0])
			matches = matches[1:]
		}
		return
	}
	if stored.Policy != a.qualityPolicy() {
		return
	}
	a.nodeIdentities = stored.Nodes
	if a.nodeIdentities == nil {
		a.nodeIdentities = make(map[string]string)
	}
	for _, record := range stored.Records {
		if record.Target == "" || len(record.Target) > 512 || record.Node == "" || len(record.Node) > 256 || record.Touched.After(a.now().Add(time.Minute)) || a.now().Sub(record.Touched) > time.Duration(a.opt.QualityTTL)*time.Second {
			continue
		}
		copy := record
		a.records[record.Target+"\x00"+record.Node] = &copy
		if record.Selection > a.sequence {
			a.sequence = record.Selection
		}
	}
}

func (a *Adaptive) expireQuality() {
	for key, record := range a.records {
		if a.now().Sub(record.Touched) > time.Duration(a.opt.QualityTTL)*time.Second {
			delete(a.records, key)
		}
	}
}

// Called with a.mu held and the current provider snapshot. Identity covers
// the parsed node map and provider name; credential/server/protocol changes
// invalidate evidence even when the user-visible name has not changed.
func (a *Adaptive) syncQualityNodes(proxies []C.Proxy) {
	a.expireQuality()
	current := make(map[string]string, len(proxies))
	for _, proxy := range proxies {
		if node, ok := proxy.(interface{ RoutingIdentity() string }); ok {
			current[proxy.Name()] = node.RoutingIdentity()
		}
	}
	for key, record := range a.records {
		// Non-persisted adapter fixtures still use process-local observations.
		if _, exists := current[record.Node]; !exists {
			found := false
			for _, p := range proxies {
				if p.Name() == record.Node {
					found = true
					break
				}
			}
			if !found {
				delete(a.records, key)
			}
		} else if current[record.Node] != a.nodeIdentities[record.Node] {
			delete(a.records, key)
		}
	}
	a.nodeIdentities = current
}

func (a *Adaptive) scheduleQuality() {
	if !a.opt.PersistQuality || a.qualityTimer != nil {
		return
	}
	a.qualityTimer = time.AfterFunc(500*time.Millisecond, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.qualityTimer = nil
		a.saveQuality()
	})
}

func (a *Adaptive) saveQuality() {
	qualityOwners.Lock()
	defer qualityOwners.Unlock()
	if qualityOwners.owners[a.qualityPath] != a.qualityOwner {
		return
	}
	a.expireQuality()
	stored := adaptiveQualityFile{Schema: 1, Policy: a.qualityPolicy(), Nodes: a.nodeIdentities}
	for _, record := range a.records {
		// Unknown identities cannot safely survive restarts.
		if a.nodeIdentities[record.Node] != "" {
			stored.Records = append(stored.Records, *record)
		}
	}
	data, err := json.Marshal(stored)
	if err != nil {
		a.qualityError = "encode-failed"
		return
	}
	dir := filepath.Dir(a.qualityPath)
	if os.MkdirAll(dir, 0700) != nil {
		a.qualityError = "directory-failed"
		return
	}
	if os.Chmod(dir, 0700) != nil {
		a.qualityError = "permissions-failed"
		return
	}
	file, err := os.CreateTemp(dir, ".pending-")
	if err != nil {
		a.qualityError = "write-failed"
		return
	}
	pending := file.Name()
	defer os.Remove(pending)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(pending, a.qualityPath)
	}
	if err != nil {
		a.qualityError = "write-failed"
		return
	}
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	a.qualityError = ""
}
