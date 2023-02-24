package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/GoAethereal/cancel"
)

const (
	// Terminated is the status returned
	// when performing an operation on a closed stream.
	Terminated Status = "terminated"
)

// Status describes the the state of a stream.
type Status string

// Error implements the builtin error interface, returning a textual representation
// of the underlying fault.
func (s Status) Error() string {
	return fmt.Sprintf("stream: status: %v", string(s))
}

// Error defines the error type of the stream package.
type Error struct {
	Status Status
	Cause  error
}

// Error implements the builtin error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%v: cause: %v", e.Status.Error(), e.Cause.Error())
}

// Unwrap implements the builtin interface for retrieving the underlying errors.
func (e Error) Unwrap() []error {
	return []error{e.Status, e.Cause}
}

// Codec defies the signature for handling a stream of data represented as byte slice.
type Codec func(buf []byte) (int, error)

// Chain combines all contained individual codecs into a single one.
// Any codec that returns the io.EOF error is considered completed and
// will the next one in line will be called.
func Chain(codecs ...Codec) Codec {
	return func(buf []byte) (int, error) {
		var n int
		for _, c := range codecs {
			var err error
			for n < len(buf) && err == nil {
				var m int
				m, err = c(buf[n:])
				n += m
			}
			if err != nil && errors.Is(err, io.EOF) {
				codecs = codecs[1:]
			} else {
				return n, err
			}
		}
		return n, io.EOF
	}
}

// Reader, by type casting, allows the conversion of a generic stream codec
// to an pkg.go.dev/io#Reader.
type Reader Codec

// Read implements pkg.go.dev/io#Reader interface.
func (r Reader) Read(buf []byte) (int, error) {
	return r(buf)
}

// Writer, by type casting, allows the conversion of a generic stream codec
// to an pkg.go.dev/io#Writer.
type Writer Codec

// Write implements pkg.go.dev/io#Writer interface.
func (w Writer) Write(buf []byte) (int, error) {
	return w(buf)
}

// Handler defines the read (rx) and write (tx) operations
// performed against a stream.
type Handler func(rx, tx Codec) error

// Executor defines the actor that performs the given command against
// an underlying stream.
type Executor func(cmd Handler) error

// Execute is syntactical sugar for evoking the underlying executor
// using the Execute keyword.
func (e Executor) Execute(cmd Handler) error {
	return e(cmd)
}

// Network is the configuration from which an Ethernet/IP executor can be derived.
type Network struct {
	// Protocol defines the underlying framing.
	// For details refer to pkg.go.dev/net#Dial network parameters.
	Protocol string

	// Endpoint defines the targeted host address.
	// For details refer to pkg.go.dev/net#Dial address parameters.
	Endpoint string

	// Watchdog defines the timeout when establishing a new connection
	// or performing read/write operations on an existing one.
	Watchdog time.Duration
}

// Client defines the instantiation of an executor serving as client.
type Client interface {
	Client(ctx cancel.Context) Executor
}

var _ Client = new(Network)

// Client returns an executor for performing network operations
// on an endpoint defined by the Network parameters.
func (cfg Network) Client(ctx cancel.Context) Executor {
	var (
		err  error
		con  net.Conn
		wrap = func(s Codec, d func(t time.Time) error) Codec {
			return func(buf []byte) (int, error) {
				if err := d(time.Now().Add(cfg.Watchdog)); err != nil {
					return 0, err
				}
				var n, err = s(buf)
				if op := (&net.OpError{}); err != nil && (errors.Is(err, io.EOF) || errors.As(err, &op) && !op.Temporary()) {
					con, err = nil, Error{Status: Terminated, Cause: err}
				}
				return n, err
			}
		}
	)
	return func(cmd Handler) error {
		select {
		case <-ctx.Done():
			if con != nil {
				if err = con.Close(); err == nil {
					con = nil
				}
			}
			return Error{Status: Terminated, Cause: errors.Join(context.Canceled, err)}
		default:
			if con == nil {
				if con, err = net.DialTimeout(cfg.Protocol, cfg.Endpoint, cfg.Watchdog); err != nil {
					return Error{Status: Terminated, Cause: err}
				}
			}
			return cmd(wrap(con.Read, con.SetReadDeadline), wrap(con.Write, con.SetWriteDeadline))
		}
	}
}
