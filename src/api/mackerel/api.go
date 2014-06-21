package mackerel

import (
	libhttp "net/http"
	httpapi "api/http"
	"fmt"
	"net"
	"coordinator"
	"cluster"
	"time"
	"encoding/json"
	"io/ioutil"
	"github.com/bmizerany/pat"
	"strings"
	"protocol"

	log "code.google.com/p/log4go"
)

type Host struct {
	Id     string `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

type Metric struct {
	HostId string `json:"hostId"`
	Name string `json:"name"`
	Time float64`json:"time"`
	Value float64 `json:"value"`
}

type MackerelServer struct {
	conn           net.Listener
	sslConn        net.Listener
	Database       string
	httpPort       string
	httpSslPort    string
	httpSslCert    string
	coordinator    coordinator.Coordinator
	userManager    httpapi.UserManager
	shutdown       chan bool
	clusterConfig  *cluster.ClusterConfiguration
	raftServer     *coordinator.RaftServer
	readTimeout    time.Duration
	user           *cluster.ClusterAdmin
}

func NewMackerelServer(database, httpPort string, readTimeout time.Duration, theCoordinator coordinator.Coordinator, userManager httpapi.UserManager, clusterConfig *cluster.ClusterConfiguration, raftServer *coordinator.RaftServer) *MackerelServer {
	self := &MackerelServer{}
	self.httpPort = httpPort
	self.Database = database
	self.coordinator = theCoordinator
	self.userManager = userManager
	self.shutdown = make(chan bool, 2)
	self.clusterConfig = clusterConfig
	self.raftServer = raftServer
	self.readTimeout = readTimeout
	self.getAuth()

	return self
}

func (self *MackerelServer) ListenAndServe() {
	var err error
	if self.httpPort != "" {
		self.conn, err = net.Listen("tcp", self.httpPort)
		if err != nil {
			log.Error("Listen: ", err)
		}
	}
	self.Serve(self.conn)
}

func (self *MackerelServer) Serve(listener net.Listener) {
	defer func() { self.shutdown <- true }()
	self.conn = listener

	r := pat.New()
	r.Get("/api/v0/hosts/:id", libhttp.HandlerFunc(self.findHost))
	r.Post("/api/v0/hosts", libhttp.HandlerFunc(self.createHost))
	r.Put("/api/v0/hosts", libhttp.HandlerFunc(self.updateHost))
	r.Post("/api/v0/tsdb", libhttp.HandlerFunc(self.registerMetric))

	self.serveListener(listener, r)
}

func (self *MackerelServer) serveListener(listener net.Listener, p *pat.PatternServeMux) {
	log.Info("Starting Mackerel api on port %s", self.httpPort)

	srv := &libhttp.Server{Handler: p, ReadTimeout: self.readTimeout}
	if err := srv.Serve(listener); err != nil && !strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
}

func (self *MackerelServer) getAuth() {
	// just use root account
	self.user = self.clusterConfig.GetClusterAdmin("root")
}

func (self *MackerelServer) findHost(w libhttp.ResponseWriter, r *libhttp.Request){
	var data struct {
		Host *Host `json:"host"`
	}

	id := r.URL.Query().Get(":id")
	data.Host = &Host{
		Id: id,
		Name: id,
		Type: "unknown",
		Status: "working",
	}

	req, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		return
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(libhttp.StatusOK)
	fmt.Fprintf(w, string(req))
}

type Spec struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Status string `json:"status"`
	Meta map[string]interface{} `json:"meta"`
	Interfaces map[string]interface{} `json:"interfaces"`
	RoleFullnames []string `json:"roleFullnames"`
}

func (self *MackerelServer) createHost(w libhttp.ResponseWriter, r *libhttp.Request){
	body, _ := ioutil.ReadAll(r.Body)

	serializedSpec := Spec{}
	err := json.Unmarshal(body, &serializedSpec)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "{\"id\": \"%s\"}", serializedSpec.Name)
}

func (self *MackerelServer) updateHost(w libhttp.ResponseWriter, r *libhttp.Request){
	body, _ := ioutil.ReadAll(r.Body)

	serializedSpec := Spec{}
	err := json.Unmarshal(body, &serializedSpec)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "{\"id\": \"%s\"}", serializedSpec.Name)
}

func (self *MackerelServer) registerMetric(w libhttp.ResponseWriter, r *libhttp.Request) {
	series, _ := ioutil.ReadAll(r.Body)

	serializedMetric := []*Metric{}
	err := json.Unmarshal(series, &serializedMetric)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		return
	}

	// convert the wire format to the internal representation of the time series
	dataStoreSeries := convertMackerelJsonToSeries(serializedMetric)
	err = self.coordinator.WriteSeriesData(self.user, self.Database, dataStoreSeries)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		return
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(libhttp.StatusOK)
	fmt.Fprintf(w, "{\"result\":\"OK\"}")
}

func convertMackerelJsonToSeries(metric []*Metric) []*protocol.Series {
	dataStoreSeries := make([]*protocol.Series, 0, len(metric))
	for _, s := range metric {
		name := fmt.Sprintf("mackerel.host.%s", s.HostId)
		t := int64(s.Time * 1000000)
		ser := &protocol.Series{
			Points: []*protocol.Point{
				&protocol.Point{
					Values: []*protocol.FieldValue{
						&protocol.FieldValue{
							DoubleValue: &s.Value,
						},
					},
					Timestamp: &t,
				},
			},
			Name: &name,
			Fields: []string {
				s.Name,
			},
		}

		dataStoreSeries = append(dataStoreSeries, ser)
	}

	return dataStoreSeries
}
