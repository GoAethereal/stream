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

const pkg = "stream"

const (
	// Terminated is the error returned,
	// when performing an operation on a closed stream.
	Terminated Status = pkg + ": terminated"
)

// Error defines the error type of the stream package.
type Error struct {
	Cause  error
	Status Status
}

// Unwrap implements the builtin interface for retrieving
// the underlying error.
func (e Error) Unwrap() error {
	return e.Cause
}

// Is implements the builtin interface for checking the
// underlying error type against the specified target.
func (e Error) Is(target error) bool {
	return e.Status == target || errors.Is(e.Cause, target)
}

// Error implements the builtin error interface.
func (e Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%v: cause: %v", e.Status, e.Cause)
	}
	return e.Status.Error()
}

// Status.
type Status string

// Error implements the builtin error interface.
func (s Status) Error() string {
	return string(s)
}

// Codec defies the signature for handling a stream of data represented as byte-slice.
type Codec func(buf []byte) (int, error)

func (c Codec) chain(next Codec) Codec {
	return func(buf []byte) (n int, err error) {
		if c == nil {
			return next(buf)
		}
		if n, err = c(buf); err != nil && errors.Is(err, io.EOF) {
			if c = nil; len(buf[n:]) > 0 {
				m, err := next(buf[n:]) // shortcut for calling next if the buffer is not consumed entirely.
				return n + m, err
			}
			return n, nil
		}
		return n, err
	}
}

type Codecs []Codec

// Merge combines all contained individual codecs into a single one.
// Any codec that returns the io.EOF error is considered completed and
// will the next one in line will be called.
func (codecs Codecs) Merge() Codec {
	var c = codecs[0]
	for _, n := range codecs[1:] {
		c.chain(n)
	}
	return c
}

// reader is internally used to convert a codec to a io.Reader.
type Reader Codec

// Read implements the io.Reader interface on a codec.
func (r Reader) Read(buf []byte) (int, error) {
	return r(buf)
}

// writer is internally used to convert a codec to a io.Writer.
type Writer Codec

// Write implements the io.Writer interface on a codec.
func (w Writer) Write(buf []byte) (int, error) {
	return w(buf)
}

// Handler
type Handler func(rx, tx Codec) error

// Executor
type Executor func(cmd Handler) error

// Execute is syntactical sugar for calling the executor.
func (e Executor) Execute(cmd Handler) error {
	return e(cmd)
}

// Network
type Network struct {
	Protocol string
	Endpoint string
	Timeout  time.Duration
}

// Client returns an executor for performing network operations
// on an endpoint defined by the Network parameters.
func (cfg Network) Client(ctx cancel.Context) Executor {
	var (
		con  net.Conn
		wrap = func(s Codec, d func(t time.Time) error) Codec {
			return func(buf []byte) (n int, err error) {
				if err = d(time.Now().Add(cfg.Timeout)); err != nil {
					return 0, err
				}
				n, err = s(buf)
				if op := (&net.OpError{}); errors.As(err, &op) && !op.Temporary() {
					con, err = nil, Error{Status: Terminated, Cause: err}
				}
				return n, err
			}
		}
	)
	// Watchdog for terminating an existing connection on cancellation.
	go func() {
		if <-ctx.Done(); con != nil {
			con.Close()
		}
	}()
	return func(cmd Handler) (err error) {
		select {
		case <-ctx.Done():
			return Error{Status: Terminated, Cause: context.Canceled}
		default:
			if con == nil {
				if con, err = net.DialTimeout(cfg.Protocol, cfg.Endpoint, cfg.Timeout); err != nil {
					return err
				}
			}
			return cmd(wrap(con.Read, con.SetReadDeadline), wrap(con.Write, con.SetWriteDeadline))
		}
	}
}
