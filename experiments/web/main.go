package main

import (
	"context"
	"encoding/json"
	"fmt"
	"idss/graphdb/common"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/multiformats/go-multiaddr"
)

// server represents the web server and its state.
type server struct {
	Root             string
	ServerDir        string
	ClientDir        string
	ClientBin        string
	ClientResultsDir string
	RuntimeDir       string
	mu               sync.Mutex
	queryMu          sync.Mutex
	totalPeers       int
	address          string
	launches         map[int]*exec.Cmd
	overlay          *overlayObserver
	progressMu       sync.Mutex
	progress         launchProgress
	historyMu        sync.Mutex
	history          []historyEntry
	managedPeerIDs   map[string]bool
}

// dataConfig represents the configuration for generating data.
// It includes the number of customers, the number of days, and the interval in minutes between data points.
const (
	maxPreviewRows    = 200
	maxHistoryEntries = 20
)

type dataConfig struct {
	Customers       int `json:"customers"`
	Days            int `json:"days"`
	IntervalMinutes int `json:"intervalMinutes"`
}

// addPeersRequest represents the request payload for adding peers.
type addPeersRequest struct {
	Count       int  `json:"count"`
	ManagerPeer bool `json:"managerPeer"`
	dataConfig
}

// queryRequest represents the request payload for executing a query.
type queryRequest struct {
	Query string  `json:"query"`
	TTL   float64 `json:"ttl"`
	Role  string  `json:"role"`
}

// statusResponse represents the response payload for the server status.
type statusResponse struct {
	Peers            int            `json:"peers"`
	ManagerPeers     int            `json:"managerPeers"`
	MemberPeers      int            `json:"memberPeers"`
	OverlayPeers     int            `json:"overlayPeers"`
	DiscoveredPeers  int            `json:"discoveredPeers"`
	DataReadyPeers   int            `json:"dataReadyPeers"`
	TotalNodesLoaded int            `json:"totalNodesLoaded"`
	TotalEdgesLoaded int            `json:"totalEdgesLoaded"`
	Address          string         `json:"address"`
	Running          bool           `json:"running"`
	ManagedPeers     int            `json:"managedPeers"`
	RuntimePath      string         `json:"runtimePath"`
	Launch           launchProgress `json:"launch"`
	ClientPath       string         `json:"clientPath"`
}

// peerInfo represents the information about a peer.
type peerInfo struct {
	ID           string  `json:"id"`
	PID          int     `json:"pid"`
	Role         string  `json:"role"`
	Address      string  `json:"address,omitempty"`
	StartedAt    string  `json:"startedAt,omitempty"`
	LifetimeSecs float64 `json:"lifetimeSeconds"`
	NodesLoaded  int     `json:"nodesLoaded"`
	EdgesLoaded  int     `json:"edgesLoaded"`
	DataReady    bool    `json:"dataReady"`
	Discovered   bool    `json:"discovered"`
	ManagedByWeb bool    `json:"managedByWeb"`
}

// dropPeersRequest represents the request payload for dropping peers.
type dropPeersRequest struct {
	IDs []string `json:"ids"`
}

// launchProgress represents the progress of launching peers.
type launchProgress struct {
	Active    bool   `json:"active"`
	Requested int    `json:"requested"`
	Ready     int    `json:"ready"`
	Stage     string `json:"stage"`
	Message   string `json:"message"`
	StartedAt string `json:"startedAt,omitempty"`
	Error     string `json:"error,omitempty"`
}

// queryResponse represents the response payload for a query.
type queryResponse struct {
	Timestamp       string     `json:"timestamp"`
	ElapsedSeconds  float64    `json:"elapsedSeconds"`
	RespondingPeers int        `json:"respondingPeers"`
	RowsReturned    int        `json:"rowsReturned"`
	Header          []string   `json:"header,omitempty"`
	Rows            [][]string `json:"rows,omitempty"`
	Truncated       bool       `json:"truncated"`
	Output          string     `json:"output,omitempty"`
}

// historyEntry represents a single entry in the query history.
type historyEntry struct {
	Timestamp       string     `json:"timestamp"`
	Query           string     `json:"query"`
	TTL             float64    `json:"ttl"`
	Role            string     `json:"role"`
	ElapsedSeconds  float64    `json:"elapsedSeconds"`
	RespondingPeers int        `json:"respondingPeers"`
	RowsReturned    int        `json:"rowsReturned"`
	Header          []string   `json:"header,omitempty"`
	Rows            [][]string `json:"rows,omitempty"`
	Truncated       bool       `json:"truncated"`
}

var (
	addressPattern          = regexp.MustCompile(`First peer address:\s*(\S+)`)
	listeningAddressPattern = regexp.MustCompile(`Listening on peer Address:\s*(/ip4/\S+)`)
	peersPattern            = regexp.MustCompile(`Responding peers:\s*(\d+)`)
	rowsPattern             = regexp.MustCompile(`Got\s+(\d+)\s+records`)
	clientElapsedPattern    = regexp.MustCompile(`Time spent:\s*([0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s|m))`)
	loadedDataPattern       = regexp.MustCompile(`Loaded (\d+) nodes and (\d+) edges into graph database`)
)

func main() {
	root := findRoot()
	if value := os.Getenv("IDSS_ROOT"); value != "" {
		root = value
	}
	runtimeDir := filepath.Join(root, "experiments", "web-runtime")
	if err := os.MkdirAll(runtimeDir, 0755); err != nil {
		panic(err)
	}

	clientBin := filepath.Join(runtimeDir, "idss-client")
	build := exec.Command("go", "build", "-o", clientBin, "./client")
	build.Dir = root
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic(fmt.Errorf("build client: %w", err))
	}

	controller := &server{
		Root: root, ServerDir: filepath.Join(root, "server"), ClientDir: filepath.Join(root, "client"),
		ClientBin: clientBin, ClientResultsDir: filepath.Join(runtimeDir, "client-results"), RuntimeDir: runtimeDir,
		launches: make(map[int]*exec.Cmd), managedPeerIDs: make(map[string]bool),
	}
	if observer, err := newOverlayObserver(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "warning: libp2p overlay observer unavailable: %v\n", err)
	} else {
		controller.overlay = observer
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", controller.index)
	mux.HandleFunc("/api/status", controller.status)
	mux.HandleFunc("/api/peers", controller.addPeers)
	mux.HandleFunc("/api/peers/list", controller.peerList)
	mux.HandleFunc("/api/peers/drop", controller.dropPeers)
	mux.HandleFunc("/api/query", controller.query)
	mux.HandleFunc("/api/history", controller.historyHandler)
	mux.HandleFunc("/api/stop", controller.stop)

	addr := os.Getenv("IDSS_WEB_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	fmt.Printf("IDSS experiment web service listening on http://localhost%s\n", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		panic(err)
	}
}

func findRoot() string {
	workingDirectory, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return workingDirectory
		}
	}
}

// overlayPeer records the last address and sighting time for a peer discovered on the IDSS overlay.
type overlayPeer struct {
	Addrs    []string
	LastSeen time.Time
}

// overlayObserver is a lightweight, non-storing libp2p client that leverages the same
// mDNS/DHT rendezvous discovery service the IDSS peers use, so overlay membership is
// reported from the actual network protocol instead of scraping local logs or processes.
type overlayObserver struct {
	host  host.Host
	dht   *dht.IpfsDHT
	mu    sync.Mutex
	peers map[peer.ID]overlayPeer
}

const overlayPeerTTL = 20 * time.Second

func newOverlayObserver(ctx context.Context) (*overlayObserver, error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("generate observer key: %w", err)
	}
	listenAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		return nil, fmt.Errorf("create observer listen address: %w", err)
	}
	observerHost, err := libp2p.New(libp2p.Identity(priv), libp2p.ListenAddrs(listenAddr), libp2p.Security(noise.ID, noise.New))
	if err != nil {
		return nil, fmt.Errorf("create observer host: %w", err)
	}
	kademliaDHT, err := dht.New(observerHost, dht.Mode(dht.ModeClient), dht.ProtocolPrefix("/idss"))
	if err != nil {
		observerHost.Close()
		return nil, fmt.Errorf("create observer DHT: %w", err)
	}
	_ = kademliaDHT.Bootstrap(ctx) // No fixed bootstrap peers; mDNS discovery still works in isolated mode.

	observer := &overlayObserver{host: observerHost, dht: kademliaDHT, peers: make(map[peer.ID]overlayPeer)}

	mdnsService := mdns.NewMdnsService(observerHost, common.DISCOVERY_SERVICE, observer)
	if err := mdnsService.Start(); err != nil {
		observerHost.Close()
		return nil, fmt.Errorf("start observer mDNS discovery: %w", err)
	}

	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, common.DISCOVERY_SERVICE)

	go observer.pollRoutingDiscovery(ctx, routingDiscovery)
	go observer.expireStalePeers(ctx)

	return observer, nil
}

// HandlePeerFound implements mdns.Notifee, recording peers discovered on the local network.
func (o *overlayObserver) HandlePeerFound(info peer.AddrInfo) {
	o.recordPeer(info)
}

func (o *overlayObserver) pollRoutingDiscovery(ctx context.Context, routingDiscovery *drouting.RoutingDiscovery) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		findCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		if peerChan, err := routingDiscovery.FindPeers(findCtx, common.DISCOVERY_SERVICE); err == nil {
			for info := range peerChan {
				o.recordPeer(info)
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (o *overlayObserver) recordPeer(info peer.AddrInfo) {
	if info.ID == o.host.ID() || len(info.Addrs) == 0 {
		return
	}
	addrStrings := make([]string, 0, len(info.Addrs))
	for _, addr := range info.Addrs {
		addrStrings = append(addrStrings, addr.Encapsulate(multiaddr.StringCast("/p2p/"+info.ID.String())).String())
	}
	o.mu.Lock()
	o.peers[info.ID] = overlayPeer{Addrs: addrStrings, LastSeen: time.Now()}
	o.mu.Unlock()
}

func (o *overlayObserver) expireStalePeers(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-overlayPeerTTL)
			o.mu.Lock()
			for id, info := range o.peers {
				if info.LastSeen.Before(cutoff) {
					delete(o.peers, id)
				}
			}
			o.mu.Unlock()
		}
	}
}

// snapshot returns the number of peers currently reachable via the overlay discovery
// service and a queryable multiaddress for one of them.
func (o *overlayObserver) snapshot() (int, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	address := ""
	for _, info := range o.peers {
		if len(info.Addrs) > 0 {
			address = info.Addrs[0]
			break
		}
	}
	return len(o.peers), address
}

func (o *overlayObserver) availablePeers() []peerInfo {
	o.mu.Lock()
	defer o.mu.Unlock()
	peers := make([]peerInfo, 0, len(o.peers))
	for id, info := range o.peers {
		address := ""
		if len(info.Addrs) > 0 {
			address = info.Addrs[0]
		}
		peers = append(peers, peerInfo{
			ID: id.String(), Role: "overlay", Address: address,
			LifetimeSecs: time.Since(info.LastSeen).Seconds(),
			StartedAt:    info.LastSeen.UTC().Format(time.RFC3339),
		})
	}
	return peers
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, page)
}

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, s.overlayStatusLocked())
}

func (s *server) setLaunchProgress(progress launchProgress) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	s.progress = progress
}

func (s *server) progressSnapshot() launchProgress {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	return s.progress
}

func (s *server) peerList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	peers := s.livePeersLocked()
	s.mu.Unlock()
	known := make(map[string]bool, len(peers))
	for _, info := range peers {
		known[info.ID] = true
	}
	if s.overlay != nil {
		for _, info := range s.overlay.availablePeers() {
			if !known[info.ID] {
				peers = append(peers, info)
			}
		}
	}
	writeJSON(w, peers)
}

func (s *server) addPeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var request addPeersRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Count < 1 || request.Customers < 1 || request.Days < 1 || request.IntervalMinutes < 1 || 1440%request.IntervalMinutes != 0 {
		http.Error(w, "count, customers, days, and intervalMinutes must be positive; intervalMinutes must divide 1440", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if request.Count < 1 {
		http.Error(w, "count must be positive", http.StatusBadRequest)
		s.mu.Unlock()
		return
	}
	startIndex := s.totalPeers
	if runningPeers := s.runningPeerCount(); runningPeers > startIndex {
		startIndex = runningPeers
	}
	logPath := filepath.Join(s.RuntimeDir, fmt.Sprintf("launcher-%d.log", time.Now().UnixNano()))
	logFile, err := os.Create(logPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		s.mu.Unlock()
		return
	}
	args := []string{"./start_peers.sh", strconv.Itoa(request.Count)}
	if request.ManagerPeer {
		args = append(args, strconv.Itoa(startIndex+1))
	}
	args = append(args, "--customers", strconv.Itoa(request.Customers), "--days", strconv.Itoa(request.Days), "--interval-minutes", strconv.Itoa(request.IntervalMinutes))
	command := exec.Command(args[0], args[1:]...)
	command.Dir = s.ServerDir
	command.Stdout = logFile
	command.Stderr = logFile
	command.Env = append(os.Environ(),
		"PEER_INDEX_OFFSET="+strconv.Itoa(startIndex),
		"PRESERVE_EXISTING_LOGS=1",
	)
	if err := command.Start(); err != nil {
		logFile.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		s.mu.Unlock()
		return
	}
	s.launches[startIndex] = command
	s.totalPeers += request.Count
	s.setLaunchProgress(launchProgress{Active: true, Requested: request.Count, Stage: "starting", Message: "Peer launcher started", StartedAt: time.Now().UTC().Format(time.RFC3339)})
	s.mu.Unlock()
	go func() {
		err := command.Wait()
		logFile.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "peer launcher %d exited: %v\n", startIndex, err)
		}
	}()

	address, ready := s.waitForLaunch(logPath, request.Count)
	if address != "" && s.address == "" {
		s.mu.Lock()
		s.address = address
		s.mu.Unlock()
	}
	if !ready {
		s.setLaunchProgress(launchProgress{Requested: request.Count, Ready: s.progressSnapshot().Ready, Stage: "failed", Message: "Peer launch failed", Error: "Peers did not become ready"})
		writeJSONStatus(w, http.StatusGatewayTimeout, map[string]string{"error": "peers did not become ready", "log": logPath})
		return
	}
	s.setLaunchProgress(launchProgress{Requested: request.Count, Ready: request.Count, Stage: "ready", Message: "All peers have joined the overlay"})
	s.mu.Lock()
	writeJSON(w, s.overlayStatusLocked())
	s.mu.Unlock()
}

func (s *server) dropPeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var request dropPeersRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.IDs) == 0 {
		http.Error(w, "ids must contain at least one peer ID or pid:<number>", http.StatusBadRequest)
		return
	}
	requested := make(map[string]bool, len(request.IDs))
	for _, id := range request.IDs {
		requested[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dropped := make([]peerInfo, 0, len(requested))
	for _, info := range s.livePeersLocked() {
		if !info.ManagedByWeb {
			continue
		}
		if !requested[info.ID] && !requested[fmt.Sprintf("pid:%d", info.PID)] {
			continue
		}
		if process, err := os.FindProcess(info.PID); err == nil {
			_ = process.Signal(os.Interrupt)
			dropped = append(dropped, info)
		}
	}
	writeJSON(w, map[string]any{"dropped": dropped, "status": s.overlayStatusLocked()})
}

func (s *server) livePeersLocked() []peerInfo {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return []peerInfo{}
	}
	peers := make([]peerInfo, 0)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || !entry.IsDir() {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil || !strings.Contains(string(cmdline), "idss_server") {
			continue
		}
		info := peerInfo{PID: pid, ID: fmt.Sprintf("pid:%d", pid), Role: "member", LifetimeSecs: processLifetime(pid)}
		if info.LifetimeSecs > 0 {
			info.StartedAt = time.Now().Add(-time.Duration(info.LifetimeSecs * float64(time.Second))).UTC().Format(time.RFC3339)
		}
		args := strings.Split(string(cmdline), "\x00")
		for _, arg := range args {
			if arg == "-manager" {
				info.Role = "manager"
			}
		}
		logPath := processLogPath(pid)
		if logPath != "" {
			info = s.enrichPeerFromLog(info, logPath)
		}
		info.ManagedByWeb = s.managedPeerIDs[info.ID]
		peers = append(peers, info)
	}
	return peers
}

func processLogPath(pid int) string {
	for _, fd := range []string{"1", "2"} {
		path, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "fd", fd))
		if err == nil && strings.HasSuffix(path, ".log") {
			return path
		}
	}
	return ""
}

func processLifetime(pid int) float64 {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(stat))
	if len(fields) <= 21 {
		return 0
	}
	startTicks, err := strconv.ParseFloat(fields[21], 64)
	if err != nil {
		return 0
	}
	uptime, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	currentSeconds, err := strconv.ParseFloat(strings.Fields(string(uptime))[0], 64)
	if err != nil {
		return 0
	}
	return currentSeconds - startTicks/100.0
}

func (s *server) enrichPeerFromLog(info peerInfo, logPath string) peerInfo {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return info
	}
	text := string(data)
	if match := listeningAddressPattern.FindStringSubmatch(text); len(match) == 2 {
		info.Address = match[1]
		parts := strings.Split(match[1], "/p2p/")
		if len(parts) == 2 {
			info.ID = parts[1]
		}
	}
	if match := loadedDataPattern.FindStringSubmatch(text); len(match) == 3 {
		info.NodesLoaded, _ = strconv.Atoi(match[1])
		info.EdgesLoaded, _ = strconv.Atoi(match[2])
	}
	info.DataReady = strings.Contains(text, "Data generation completed")
	info.Discovered = strings.Contains(text, "Peer discovery completed")
	return info
}

func (s *server) waitForLaunch(logPath string, count int) (string, bool) {
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(logPath)
		text := string(data)
		ready := len(regexp.MustCompile(`Peer [0-9]+ launched with ID `).FindAllString(text, -1))
		for _, match := range regexp.MustCompile(`Peer [0-9]+ launched with ID (\S+)`).FindAllStringSubmatch(text, -1) {
			s.mu.Lock()
			s.managedPeerIDs[match[1]] = true
			s.mu.Unlock()
		}
		stage := "starting"
		message := fmt.Sprintf("Starting peers: %d/%d ready", ready, count)
		if strings.Contains(text, "Starting peer batch") {
			stage = "loading"
		}
		if strings.Contains(text, "All peers have joined the overlay.") {
			stage = "ready"
			message = "All peers have joined the overlay"
		}
		s.setLaunchProgress(launchProgress{Active: stage != "ready", Requested: count, Ready: ready, Stage: stage, Message: message, StartedAt: s.progressSnapshot().StartedAt})
		address := ""
		if match := addressPattern.FindStringSubmatch(text); len(match) == 2 {
			address = match[1]
		}
		if strings.Contains(text, "All peers have joined the overlay.") {
			return address, true
		}
		if strings.Contains(text, "failed") || strings.Contains(text, "Failed") {
			s.setLaunchProgress(launchProgress{Requested: count, Ready: ready, Stage: "failed", Message: "Peer launch failed", Error: "See launcher log"})
			return address, false
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", false
}

func (s *server) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var request queryRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Query == "" || request.TTL < 1 {
		http.Error(w, "query is required and ttl must be at least 1", http.StatusBadRequest)
		return
	}
	if request.Role == "" {
		request.Role = "manager"
	}

	s.mu.Lock()
	overlay := s.overlayStatusLocked()
	address := overlay.Address
	running := overlay.Running
	s.mu.Unlock()
	if address == "" || !running {
		http.Error(w, "start peers before querying", http.StatusConflict)
		return
	}

	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	result, err := s.runQuery(address, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	s.recordHistory(request, result)
	writeJSON(w, result)
}

func (s *server) historyHandler(w http.ResponseWriter, r *http.Request) {
	s.historyMu.Lock()
	defer s.historyMu.Unlock()
	history := s.history
	if history == nil {
		history = []historyEntry{}
	}
	writeJSON(w, history)
}

func (s *server) recordHistory(request queryRequest, result queryResponse) {
	entry := historyEntry{
		Timestamp: result.Timestamp, Query: request.Query, TTL: request.TTL, Role: request.Role,
		ElapsedSeconds: result.ElapsedSeconds, RespondingPeers: result.RespondingPeers,
		RowsReturned: result.RowsReturned, Header: result.Header, Rows: result.Rows, Truncated: result.Truncated,
	}
	s.historyMu.Lock()
	defer s.historyMu.Unlock()
	s.history = append([]historyEntry{entry}, s.history...)
	if len(s.history) > maxHistoryEntries {
		s.history = s.history[:maxHistoryEntries]
	}
}

func (s *server) runQuery(address string, request queryRequest) (queryResponse, error) {
	_ = os.RemoveAll(filepath.Join(s.ClientDir, "results"))
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, s.ClientBin, "-role", request.Role, "-s", address)
	command.Dir = s.ClientDir
	command.Env = append(os.Environ(), "IDSS_CLIENT_RESULTS_DIR="+s.ClientResultsDir)
	command.Stdin = strings.NewReader(fmt.Sprintf("%s, %s\nexit\n", request.Query, strconv.FormatFloat(request.TTL, 'f', -1, 64)))
	output, err := command.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return queryResponse{}, fmt.Errorf("client query timed out after %s", 2*time.Minute)
		}
		return queryResponse{}, fmt.Errorf("client query failed: %w", err)
	}
	clientOutput := string(output)
	result := queryResponse{Timestamp: time.Now().UTC().Format(time.RFC3339), ElapsedSeconds: time.Since(started).Seconds(), Output: clientOutput}
	if match := clientElapsedPattern.FindStringSubmatch(clientOutput); len(match) == 2 {
		if elapsed, parseErr := time.ParseDuration(match[1]); parseErr == nil {
			result.ElapsedSeconds = elapsed.Seconds()
		}
	}
	if match := peersPattern.FindStringSubmatch(clientOutput); len(match) == 2 {
		result.RespondingPeers, _ = strconv.Atoi(match[1])
	}
	if match := rowsPattern.FindStringSubmatch(clientOutput); len(match) == 2 {
		result.RowsReturned, _ = strconv.Atoi(match[1])
	}
	files, _ := filepath.Glob(filepath.Join(s.ClientDir, "results", "*.json"))
	if len(files) == 0 {
		return result, fmt.Errorf("client returned no result JSON: %s", strings.TrimSpace(string(output)))
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		return result, err
	}
	var parsed struct {
		ResultCount int        `json:"resultCount"`
		Header      []string   `json:"header"`
		Rows        [][]string `json:"rows"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return result, err
	}
	result.RowsReturned = parsed.ResultCount
	result.Header = parsed.Header
	result.Rows = parsed.Rows
	if len(result.Rows) > maxPreviewRows {
		result.Rows = result.Rows[:maxPreviewRows]
		result.Truncated = true
	}
	return result, nil
}

func (s *server) stop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, command := range s.launches {
		if command.Process != nil && s.processRunning(command) {
			_ = command.Process.Signal(os.Interrupt)
		}
	}
	s.totalPeers = 0
	s.address = ""
	writeJSON(w, s.overlayStatusLocked())
}

func (s *server) overlayStatusLocked() statusResponse {
	livePeers := s.livePeersLocked()
	managerCount, memberCount := 0, 0
	nodes, edges, dataReady := 0, 0, 0
	discovered := 0
	for _, peer := range livePeers {
		if peer.Role == "manager" {
			managerCount++
		} else {
			memberCount++
		}
		nodes += peer.NodesLoaded
		edges += peer.EdgesLoaded
		if peer.DataReady {
			dataReady++
		}
		if peer.Discovered {
			discovered++
		}
	}

	overlayPeers, overlayAddress := 0, ""
	if s.overlay != nil {
		overlayPeers, overlayAddress = s.overlay.snapshot()
	}
	// Prefer the address the overlay's own discovery service found; fall back to the
	// launcher-reported address, then to log scraping if the observer has nothing yet.
	address := overlayAddress
	if address == "" && len(livePeers) > 0 {
		address = s.address
	}

	return statusResponse{
		Peers:            len(livePeers),
		ManagerPeers:     managerCount,
		MemberPeers:      memberCount,
		OverlayPeers:     overlayPeers,
		DiscoveredPeers:  discovered,
		DataReadyPeers:   dataReady,
		TotalNodesLoaded: nodes,
		TotalEdgesLoaded: edges,
		Address:          address,
		Running:          overlayPeers > 0 || len(livePeers) > 0,
		ManagedPeers:     s.totalPeers,
		RuntimePath:      s.RuntimeDir,
		ClientPath:       s.ClientBin,
		Launch:           s.progressSnapshot(),
	}
}

// peerProcessCmdlines returns the raw /proc cmdline contents of every running idss_server process.
func (s *server) peerProcessCmdlines() []string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var cmdlines []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		commandLine, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err == nil && strings.Contains(string(commandLine), "idss_server") {
			cmdlines = append(cmdlines, string(commandLine))
		}
	}
	return cmdlines
}

func (s *server) runningPeerCount() int {
	return len(s.peerProcessCmdlines())
}

// peerRoleCounts classifies peer processes as manager or member based on the -manager flag.
func peerRoleCounts(cmdlines []string) (managerCount int, memberCount int) {
	for _, cmdline := range cmdlines {
		isManager := false
		for _, arg := range strings.Split(cmdline, "\x00") {
			if arg == "-manager" {
				isManager = true
				break
			}
		}
		if isManager {
			managerCount++
		} else {
			memberCount++
		}
	}
	return managerCount, memberCount
}

func (s *server) dataReadyPeerCount() int {
	files, _ := filepath.Glob(filepath.Join(s.ServerDir, "logs", "*.log"))
	count := 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err == nil && strings.Contains(string(data), "Data generation completed") {
			count++
		}
	}
	return count
}

// loadedDataTotals sums the node/edge counts EliasDB reported loading for each peer log.
func (s *server) loadedDataTotals() (nodes int, edges int) {
	files, _ := filepath.Glob(filepath.Join(s.ServerDir, "logs", "*.log"))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		match := loadedDataPattern.FindStringSubmatch(string(data))
		if len(match) == 3 {
			n, _ := strconv.Atoi(match[1])
			e, _ := strconv.Atoi(match[2])
			nodes += n
			edges += e
		}
	}
	return nodes, edges
}

func (s *server) discoveredPeerCount() int {
	files, _ := filepath.Glob(filepath.Join(s.ServerDir, "logs", "*.log"))
	count := 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err == nil && strings.Contains(string(data), "Peer discovery completed") {
			count++
		}
	}
	return count
}

func (s *server) firstPeerAddress() string {
	files, _ := filepath.Glob(filepath.Join(s.ServerDir, "logs", "*.log"))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		if match := listeningAddressPattern.FindStringSubmatch(string(data)); len(match) == 2 {
			return match[1]
		}
	}
	return ""
}

func (s *server) runningLocked() bool {
	for _, command := range s.launches {
		if command.Process != nil && s.processRunning(command) {
			return true
		}
	}
	return false
}

func (s *server) processRunning(command *exec.Cmd) bool {
	return command.Process.Signal(os.Signal(syscall.Signal(0))) == nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	writeJSON(w, value)
}

const page = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>IDSS Experiment Console</title>
<style>
:root{font-family:Georgia,serif;color:#17221d;background:#e9eee8}body{margin:0}.wrap{max-width:1200px;margin:auto;padding:32px}header{display:flex;justify-content:space-between;align-items:end;border-bottom:2px solid #17221d;padding-bottom:18px}h1{font-size:42px;line-height:1;margin:0}p{color:#59645d}.grid{display:grid;grid-template-columns:330px 1fr;gap:22px;margin-top:22px}.panel{background:#f8faf6;border:1px solid #b8c2b8;border-radius:8px;padding:20px;box-shadow:4px 4px 0 #c5d0c4}label{display:block;font:12px monospace;text-transform:uppercase;letter-spacing:.08em;margin:14px 0 5px}input,textarea,select,button{font:16px monospace;box-sizing:border-box;width:100%;padding:10px;border:1px solid #879489;border-radius:4px;background:white}textarea{min-height:100px;resize:vertical}button{background:#d4643f;color:white;border:0;cursor:pointer;margin-top:16px}button.secondary{background:#476b61}pre{white-space:pre-wrap;word-break:break-word;background:#17221d;color:#d8ead8;padding:16px;border-radius:4px;min-height:120px;font-size:12px}.status{font:13px monospace;color:#476b61}.address{font:12px monospace;color:#476b61;margin-top:6px}.overlay-metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:18px 0}.overlay-metrics .metric{background:white;border:1px solid #b8c2b8;border-radius:6px;padding:10px}.metrics{display:flex;gap:24px;margin:18px 0}.metric strong{display:block;font-size:26px;color:#d4643f}.metric span{font:11px monospace;text-transform:uppercase;color:#59645d}table{width:100%;border-collapse:collapse;font-size:13px;margin-top:12px;background:white}th,td{border:1px solid #b8c2b8;padding:6px 8px;text-align:left}th{background:#dfe6da}.result-note{font:12px monospace;color:#476b61;margin-top:8px}.table-scroll{overflow-x:auto;max-width:100%}.peer-table{overflow-x:auto;max-width:100%}.topology-wrap{overflow-x:auto;max-width:100%;background:white;border:1px solid #b8c2b8;border-radius:6px;padding:12px}.progress-bar{background:#dfe6da;border:1px solid #b8c2b8;border-radius:6px;height:18px;overflow:hidden;margin:8px 0 4px}.progress-bar-fill{height:100%;background:#476b61;width:0%;transition:width .3s ease}.progress-bar-fill.failed{background:#c0392b}.progress-bar-fill.ready{background:#3f8f5c}.toggle-row{display:flex;gap:10px;align-items:center;margin:14px 0}.toggle-row button{width:auto;margin-top:0}ul.history{list-style:none;padding:0;margin:0;max-height:260px;overflow:auto}ul.history li{border-bottom:1px solid #dfe6da;padding:8px 0}ul.history button{width:auto;background:none;border:none;color:#17221d;text-decoration:underline;cursor:pointer;padding:0;margin:0;font:13px monospace;text-align:left}ul.history .meta{display:block;font:11px monospace;color:#59645d;margin-top:2px}@media(max-width:750px){.grid{grid-template-columns:1fr}.overlay-metrics{grid-template-columns:repeat(2,1fr)}h1{font-size:32px}}
</style></head><body><main class="wrap"><header><div><h1>IDSS / field console</h1><p>Grow a live peer set, then measure distributed queries.</p></div><span id="overlayState" class="status">Checking overlay...</span></header>
<div id="overlayMetrics" class="overlay-metrics"></div>
<div id="overlayAddress" class="address"></div>
<div class="result-note">"Manager peers (running)" counts overlay processes started with -manager. Sample data separately creates one "manager" Customer record per community, so query results can show many manager-tagged customers even with a single manager peer running.</div>
<div id="clientPath" class="result-note"></div>
<div id="launchProgressBar" class="progress-bar" style="display:none"><div id="launchProgressFill" class="progress-bar-fill"></div></div>
<div id="launchProgress" class="result-note"></div>
<div class="toggle-row"><button id="toggleInventoryBtn" class="secondary" onclick="toggleInventory()">Show live inventory</button><button id="toggleTopologyBtn" class="secondary" onclick="toggleTopology()">Show topology</button></div>
<section class="panel peer-panel" id="peerPanel" style="display:none"><h2>Live peer inventory</h2><p class="result-note">Updated every 3 seconds while this panel is open. Nodes and edges above are summed only from live local peers.</p><label>Filter peers</label><input id="peerFilter" placeholder="ID, role, PID, address" oninput="renderPeers(window.__peers||[])"><label>Role</label><select id="peerRoleFilter" onchange="renderPeers(window.__peers||[])"><option value="">all roles</option><option value="manager">manager</option><option value="member">member</option><option value="overlay">overlay</option></select><div class="peer-table"><table><thead><tr><th><input id="selectAll" type="checkbox" onchange="toggleAllPeers(this.checked)"></th><th>Peer</th><th>Role</th><th>Lifetime</th><th>Nodes</th><th>Edges</th><th>Ready</th><th>Discovery</th><th>Address</th></tr></thead><tbody id="peerRows"><tr><td colspan="9">No live peers.</td></tr></tbody></table></div><button class="danger" onclick="dropSelectedPeers()">Drop selected peers</button></section>
<section class="panel" id="topologyPanel" style="display:none"><h2>Overlay topology</h2><p class="result-note">Each local peer dataset is shown as a separate community. Orange nodes are managers and green nodes are members.</p><label>Community view</label><select id="topologyCommunity" onchange="renderTopology(window.__peers||[])"><option value="all">all communities</option></select><div id="topologySvg" class="topology-wrap"></div></section>
<div class="grid"><section class="panel"><h2>Peer set</h2><label>Additional peers</label><input id="count" type="number" min="1" value="2"><label>Customers per peer</label><input id="customers" type="number" min="1" value="4"><label>Reading days</label><input id="days" type="number" min="1" value="1"><label>Interval minutes</label><input id="interval" type="number" min="1" value="15"><label><input id="managerPeer" type="checkbox" checked> Launch first added peer as manager</label><button onclick="addPeers()">Add peers</button><button class="secondary" onclick="stopPeers()">Stop managed peers</button><h2>Recent queries</h2><ul id="historyList" class="history"></ul></section>
<section class="panel"><h2>Query bench</h2><label>Query</label><textarea id="query">get Customer</textarea><label>TTL</label><input id="ttl" type="number" min="1" value="1"><label>Role</label><select id="role"><option>manager</option><option>observer</option><option>member</option></select><button onclick="runQuery()">Run query</button><div class="metrics"><div class="metric"><strong id="elapsed">-</strong><span>seconds</span></div><div class="metric"><strong id="responding">-</strong><span>peers</span></div><div class="metric"><strong id="rows">-</strong><span>rows</span></div></div><div id="resultNote" class="result-note"></div><div id="resultsTable"></div><pre id="output">No query yet.</pre></section></div></main><script>
function escapeHtml(value){return String(value).replace(/[&<>"']/g,function(c){return({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]})}
async function api(path,body){let r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});let data=await r.json();if(!r.ok)throw Error(data.error||JSON.stringify(data));return data}
function renderMetrics(d){
  overlayMetrics.innerHTML=''
    +'<div class="metric"><strong>'+d.peers+'</strong><span>total peers</span></div>'
    +'<div class="metric"><strong>'+d.managerPeers+'</strong><span>manager peers (running)</span></div>'
    +'<div class="metric"><strong>'+d.memberPeers+'</strong><span>members</span></div>'
    +'<div class="metric"><strong>'+d.overlayPeers+'</strong><span>overlay peers (libp2p)</span></div>'
    +'<div class="metric"><strong>'+d.discoveredPeers+'</strong><span>discovered</span></div>'
    +'<div class="metric"><strong>'+d.dataReadyPeers+'</strong><span>data ready</span></div>'
    +'<div class="metric"><strong>'+d.totalNodesLoaded+'</strong><span>nodes loaded</span></div>'
    +'<div class="metric"><strong>'+d.totalEdgesLoaded+'</strong><span>edges loaded</span></div>'
    +'<div class="metric"><strong>'+d.managedPeers+'</strong><span>managed here</span></div>';
  overlayAddress.textContent = d.address ? ('First peer address: '+d.address) : 'No overlay address discovered yet.';
	clientPath.textContent = d.clientPath ? ('Query client: '+d.clientPath+' (launched for each query)') : '';
	launchProgress.textContent = d.launch && (d.launch.active || d.launch.stage === 'failed' || d.launch.stage === 'ready') ? d.launch.message : '';
	renderLaunchProgress(d.launch);
}
function renderLaunchProgress(launch){
  let show = launch && (launch.active || launch.stage === 'failed' || launch.stage === 'ready');
  launchProgressBar.style.display = show ? '' : 'none';
  if(!show)return;
  let pct = launch.requested>0 ? Math.min(100, Math.round(100*launch.ready/launch.requested)) : (launch.stage==='ready'?100:0);
  launchProgressFill.style.width = pct+'%';
  launchProgressFill.className = 'progress-bar-fill' + (launch.stage==='failed'?' failed':launch.stage==='ready'?' ready':'');
}
let inventoryVisible=false, topologyVisible=false;
function toggleInventory(){
  inventoryVisible=!inventoryVisible;
  peerPanel.style.display=inventoryVisible?'':'none';
  toggleInventoryBtn.textContent=inventoryVisible?'Hide live inventory':'Show live inventory';
  if(inventoryVisible)loadPeers();
}
function toggleTopology(){
  topologyVisible=!topologyVisible;
  topologyPanel.style.display=topologyVisible?'':'none';
  toggleTopologyBtn.textContent=topologyVisible?'Hide topology':'Show topology';
  if(topologyVisible)loadTopology();
}
function communityName(peer){return peer.role==='overlay'?'Unassigned overlay peers':'Community '+peer.id.slice(0,8)}
function renderTopology(list){
	let groups={};
	list.forEach(function(peer){let name=communityName(peer);(groups[name]||(groups[name]=[])).push(peer)});
	let names=Object.keys(groups).sort(), selected=topologyCommunity.value||'all';
	topologyCommunity.innerHTML='<option value="all">all communities</option>'+names.map(function(name){return '<option value="'+escapeHtml(name)+'">'+escapeHtml(name)+'</option>'}).join('');
	topologyCommunity.value=names.indexOf(selected)>=0?selected:'all';
	let visible=topologyCommunity.value==='all'?names:names.filter(function(name){return name===topologyCommunity.value});
	if(!visible.length){topologySvg.innerHTML='<p class="result-note">No peers to draw yet.</p>';return}
	let width=680, groupHeight=300, height=groupHeight*visible.length;
	let parts=['<svg width="'+width+'" height="'+height+'" viewBox="0 0 '+width+' '+height+'">'];
	visible.forEach(function(name,groupIndex){
		let peers=groups[name], managers=peers.filter(function(p){return p.role==='manager'}), members=peers.filter(function(p){return p.role!=='manager'});
		let cx=width/2, cy=groupIndex*groupHeight+160;
		parts.push('<rect x="8" y="'+(groupIndex*groupHeight+8)+'" width="'+(width-16)+'" height="'+(groupHeight-16)+'" rx="10" fill="#f8faf6" stroke="#b8c2b8"/>');
		parts.push('<text x="24" y="'+(groupIndex*groupHeight+34)+'" font-size="14" font-weight="bold" fill="#17221d">'+escapeHtml(name)+'</text>');
		let managerPositions=managers.map(function(_,i){let angle=managers.length>1?(2*Math.PI*i/managers.length):0;let radius=managers.length>1?45:0;return {x:cx+radius*Math.cos(angle),y:cy+radius*Math.sin(angle)};});
		if(!managerPositions.length)managerPositions=[{x:cx,y:cy}];
		members.forEach(function(peer,i){let angle=2*Math.PI*i/Math.max(members.length,1);let x=cx+110*Math.cos(angle),y=cy+90*Math.sin(angle);let manager=managerPositions[i%managerPositions.length];parts.push('<line x1="'+manager.x+'" y1="'+manager.y+'" x2="'+x+'" y2="'+y+'" stroke="#b8c2b8" stroke-width="1"/>');parts.push('<circle cx="'+x+'" cy="'+y+'" r="8" fill="#476b61"><title>'+escapeHtml(peer.id)+'</title></circle>');});
		managerPositions.forEach(function(position,i){let manager=managers[i];parts.push('<circle cx="'+position.x+'" cy="'+position.y+'" r="16" fill="#d4643f"><title>'+(manager?escapeHtml(manager.id):'manager')+'</title></circle>');});
	});
	parts.push('</svg>');
	topologySvg.innerHTML=parts.join('');
}
async function loadTopology(){try{let list=window.__peers&&window.__peers.length?window.__peers:await fetch('/api/peers/list').then(function(r){return r.json()});window.__peers=list;renderTopology(list)}catch(e){}}
async function refresh(){
	try{let d=await fetch('/api/status').then(function(r){return r.json()});overlayState.textContent=d.peers>0?(d.peers+' local peers online'):(d.overlayPeers>0?(d.overlayPeers+' overlay peers discovered'):'No local peers');renderMetrics(d);if(inventoryVisible)loadPeers();if(topologyVisible)loadTopology()}
  catch(e){overlayState.textContent='unavailable'}
}
function formatLifetime(seconds){if(seconds<60)return seconds.toFixed(0)+'s';if(seconds<3600)return (seconds/60).toFixed(1)+'m';return (seconds/3600).toFixed(1)+'h'}
function renderPeers(list){
	let text=(peerFilter.value||'').toLowerCase(), selected=new Set(Array.from(document.querySelectorAll('.peerSelect:checked')).map(function(box){return box.value})), roleValue=peerRoleFilter.value;
	let filtered=list.filter(function(p){let hay=(p.id+' '+p.role+' '+p.pid+' '+(p.address||'')).toLowerCase();return (!text||hay.indexOf(text)>=0)&&(!roleValue||p.role===roleValue)});
	peerRows.innerHTML=filtered.length?filtered.map(function(p){return '<tr><td><input class="peerSelect" type="checkbox" value="'+escapeHtml(p.id)+'" '+(selected.has(p.id)?'checked':'')+'></td><td title="'+escapeHtml(p.address||'')+'">'+escapeHtml(p.id.slice(0,12))+'</td><td>'+escapeHtml(p.role)+'</td><td>'+formatLifetime(p.lifetimeSeconds)+'</td><td>'+p.nodesLoaded+'</td><td>'+p.edgesLoaded+'</td><td>'+(p.dataReady?'yes':'no')+'</td><td>'+(p.discovered?'yes':'no')+'</td><td>'+escapeHtml(p.address||'-')+'</td></tr>'}).join(''):'<tr><td colspan="9">No matching peers.</td></tr>';
}
async function loadPeers(){try{let list=await fetch('/api/peers/list').then(function(r){return r.json()});window.__peers=list;renderPeers(list)}catch(e){}}
function toggleAllPeers(checked){document.querySelectorAll('.peerSelect').forEach(function(box){box.checked=checked})}
async function dropSelectedPeers(){let ids=Array.from(document.querySelectorAll('.peerSelect:checked')).map(function(box){return box.value});if(!ids.length){return}if(!confirm('Drop '+ids.length+' selected peer(s)?'))return;try{await api('/api/peers/drop',{ids:ids});refresh()}catch(e){output.textContent=e}}
async function addPeers(){try{let d=await api('/api/peers',{count:+count.value,customers:+customers.value,days:+days.value,intervalMinutes:+interval.value,managerPeer:managerPeer.checked});overlayState.textContent=d.peers+' peers online';renderMetrics(d)}catch(e){output.textContent=e}}
async function stopPeers(){try{await api('/api/stop',{});refresh()}catch(e){output.textContent=e}}
function renderTable(header,rows){
  if(!header||!header.length){resultsTable.innerHTML='<p class="result-note">No tabular result.</p>';return}
  let html='<div class="table-scroll"><table><thead><tr>'+header.map(function(h){return '<th>'+escapeHtml(h)+'</th>'}).join('')+'</tr></thead><tbody>';
  (rows||[]).forEach(function(row){html+='<tr>'+row.map(function(c){return '<td>'+escapeHtml(c)+'</td>'}).join('')+'</tr>'});
  html+='</tbody></table></div>';
  resultsTable.innerHTML=html;
}
function applyResult(d){
  elapsed.textContent=d.elapsedSeconds.toFixed(3);
  responding.textContent=d.respondingPeers;
  rows.textContent=d.rowsReturned;
  resultNote.textContent=d.truncated?('Preview limited to first '+(d.rows?d.rows.length:0)+' rows of '+d.rowsReturned+'.'):'';
  renderTable(d.header,d.rows);
  output.textContent=d.output||'';
}
async function runQuery(){
  try{let d=await api('/api/query',{query:query.value,ttl:+ttl.value,role:role.value});applyResult(d);loadHistory()}
  catch(e){output.textContent=e}
}
function renderHistory(list){
  window.__history=list;
  historyList.innerHTML=list.map(function(entry,index){
    return '<li><button onclick="showHistory('+index+')">'+escapeHtml(entry.query)+'</button>'
      +'<span class="meta">ttl '+entry.ttl+' &middot; '+entry.role+' &middot; '+entry.rowsReturned+' rows &middot; '+entry.elapsedSeconds.toFixed(2)+'s</span></li>';
  }).join('') || '<li class="meta">No queries yet.</li>';
}
function showHistory(index){
  let entry=window.__history[index];
  if(!entry)return;
  applyResult({elapsedSeconds:entry.elapsedSeconds,respondingPeers:entry.respondingPeers,rowsReturned:entry.rowsReturned,header:entry.header,rows:entry.rows,truncated:entry.truncated,output:''});
}
async function loadHistory(){try{let list=await fetch('/api/history').then(function(r){return r.json()});renderHistory(list)}catch(e){}}
refresh();loadHistory();setInterval(refresh,3000)
</script></body></html>`
