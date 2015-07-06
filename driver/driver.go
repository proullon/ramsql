package ramsql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/proullon/ramsql/engine"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/protocol"
)

func init() {
	sql.Register("ramsql", &Driver{})
	log.SetLevel(log.WarningLevel)
}

// Driver is the driver entrypoint,
// implementing database/sql/driver interface
type Driver struct {
	// // pool is the pool of active connection to server
	// pool []driver.Conn
	endpoint protocol.DriverEndpoint

	// server is the engine instance started by driver
	server *engine.Engine
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
func (rs *Driver) Open(uri string) (conn driver.Conn, err error) {
	connConf, err := parseConnectionURI(uri)
	if err != nil {
		return nil, err
	}

	if rs.server == nil {
		driverEndpoint, engineEndpoint, err := endpoints(connConf)
		if err != nil {
			return nil, err
		}

		if rs.server, err = engine.New(engineEndpoint); err != nil {
			return nil, err
		}
		rs.endpoint = driverEndpoint

		driverConn, err := driverEndpoint.New(uri)
		if err != nil {
			return nil, err
		}

		return newConn(driverConn), nil
	}

	driverConn, err := rs.endpoint.New(uri)
	return newConn(driverConn), nil
}

func endpoints(conf *connConf) (protocol.DriverEndpoint, protocol.EngineEndpoint, error) {
	switch conf.Proto {
	default:
		driver, engine := protocol.NewChannelEndpoints()
		return driver, engine, nil
	}
}

// The uri need to have the following syntax:
//
//   [PROTOCOL_SPECFIIC*]DBNAME/USER/PASSWD
//
// where protocol spercific part may be empty (this means connection to
// local server using default protocol). Currently possible forms:
//
//   DBNAME/USER/PASSWD
//   unix:SOCKPATH*DBNAME/USER/PASSWD
//   unix:SOCKPATH,OPTIONS*DBNAME/USER/PASSWD
//   tcp:ADDR*DBNAME/USER/PASSWD
//   tcp:ADDR,OPTIONS*DBNAME/USER/PASSWD
//   cloudsql:INSTANCE*DBNAME/USER/PASSWD
//
// OPTIONS can contain comma separated list of options in form:
//   opt1=VAL1,opt2=VAL2,boolopt3,boolopt4
// Currently implemented options:
//   laddr   - local address/port (eg. 1.2.3.4:0)
//   timeout - connect timeout in format accepted by time.ParseDuration
func parseConnectionURI(uri string) (*connConf, error) {
	c := &connConf{}

	if uri == "" {
		c.Proto = "tcp"
		return c, nil
	}

	pd := strings.SplitN(uri, "*", 2)
	if len(pd) == 2 {
		// Parse protocol part of URI
		p := strings.SplitN(pd[0], ":", 2)
		if len(p) != 2 {
			return nil, errors.New("Wrong protocol part of URI")
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
		return nil, errors.New("Wrong database part of URI")
	}

	c.Db = dup[0]
	c.User = dup[1]
	c.Password = dup[2]
	return c, nil
}
