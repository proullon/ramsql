package ramsql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/proullon/ramsql/engine/executor"
	"github.com/proullon/ramsql/engine/log"
)

func init() {
	sql.Register("ramsql", NewDriver())
	log.SetLevel(log.WarningLevel)
}

// Server structs holds engine for each sql.DB instance.
// This way a sql.DB cann open as much connection to engine as wanted
// without colliding with another engine (during tests for example)
// with the unique constraint of providing a unique DataSourceName
type Server struct {
	server *executor.Engine

	// Kill server on last connection closing
	sync.Mutex
	connCount int64
}

// Driver is the driver entrypoint
//
// Drivers should implement Connector and DriverContext interaces.
//
// https://pkg.go.dev/database/sql/driver#Connector
// https://pkg.go.dev/database/sql/driver#DriverContext
type Driver struct {
	// Mutex protect the map of Server
	sync.Mutex
	// Holds all matching sql.DB instances of RamSQL engine
	servers map[string]*Server
}

// NewDriver creates a driver object
func NewDriver() *Driver {
	d := &Driver{}
	d.servers = make(map[string]*Server)
	return d
}

type connConf struct {
	Proto    string
	Addr     string
	Laddr    string
	Db       string
	Password string
	User     string
	Timeout  time.Duration
}

// Open return an active connection so RamSQL server
// If there is no connection in pool, start a new server.
// After first instantiation of the server,
func (rs *Driver) Open(dsn string) (conn driver.Conn, err error) {
	rs.Lock()
	defer rs.Unlock()

	_, err = parseConnectionURI(dsn)
	if err != nil {
		return nil, err
	}

	dsnServer, exist := rs.servers[dsn]
	if !exist {
		server, err := executor.NewEngine()
		if err != nil {
			return nil, err
		}

		s := &Server{
			server: server,
		}
		rs.servers[dsn] = s

		return newConn(s), nil
	}

	return newConn(dsnServer), err
}

// The uri need to have the following syntax:
//
//	[PROTOCOL_SPECFIIC*]DBNAME/USER/PASSWD
//
// where protocol spercific part may be empty (this means connection to
// local server using default protocol). Currently possible forms:
//
//	DBNAME/USER/PASSWD
//	unix:SOCKPATH*DBNAME/USER/PASSWD
//	unix:SOCKPATH,OPTIONS*DBNAME/USER/PASSWD
//	tcp:ADDR*DBNAME/USER/PASSWD
//	tcp:ADDR,OPTIONS*DBNAME/USER/PASSWD
//	cloudsql:INSTANCE*DBNAME/USER/PASSWD
//
// OPTIONS can contain comma separated list of options in form:
//
//	opt1=VAL1,opt2=VAL2,boolopt3,boolopt4
//
// Currently implemented options:
//
//	laddr   - local address/port (eg. 1.2.3.4:0)
//	timeout - connect timeout in format accepted by time.ParseDuration
func parseConnectionURI(uri string) (*connConf, error) {
	c := &connConf{}

	if uri == "" {
		log.Info("Empty data source name, using 'default' engine")
		uri = "default"
	}

	pd := strings.SplitN(uri, "*", 2)
	if len(pd) == 2 {
		// Parse protocol part of URI
		p := strings.SplitN(pd[0], ":", 2)
		if len(p) != 2 {
			// Wrong protocol part of URI
			return c, nil
		}
		c.Proto = p[0]
		options := strings.Split(p[1], ",")
		c.Addr = options[0]
		for _, o := range options[1:] {
			kv := strings.SplitN(o, "=", 2)
			var k, v string
			if len(kv) == 2 {
				k, v = kv[0], kv[1]
			} else {
				k, v = o, "true"
			}
			switch k {
			case "laddr":
				c.Laddr = v
			case "timeout":
				to, err := time.ParseDuration(v)
				if err != nil {
					return nil, err
				}
				c.Timeout = to
			default:
				return nil, errors.New("Unknown option: " + k)
			}
		}
		// Remove protocol part
		pd = pd[1:]
	}
	// Parse database part of URI
	dup := strings.SplitN(pd[0], "/", 3)
	if len(dup) != 3 {
		// Wrong database part of URI
		return c, nil
	}

	c.Db = dup[0]
	c.User = dup[1]
	c.Password = dup[2]
	return c, nil
}

func (s *Server) openingConn() {

	s.Lock()
	defer s.Unlock()
	s.connCount++
}

func (s *Server) closingConn() {
	s.Lock()
	defer s.Unlock()
	s.connCount--

	if s.connCount == 0 {
		s.server.Stop()
	}
}
