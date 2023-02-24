package stream_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/GoAethereal/cancel"
	. "github.com/GoAethereal/stream"
)

func validate(t *testing.T, fn func() bool, args ...any) {
	if fn() {
		t.Error(args...)
	}
}

func TestStatus(t *testing.T) {
	{ // Error formatting.
		var res = Terminated.Error()
		validate(t, func() bool {
			return res != fmt.Sprintf("stream: status: %v", string(Terminated))
		}, res)
	}
}

func TestError(t *testing.T) {
	var err = Error{Status: Terminated, Cause: io.EOF}
	{ // Error formatting.
		var res = err.Error()
		validate(t, func() bool {
			return res != fmt.Sprintf("%v: cause: %v", Terminated.Error(), io.EOF.Error())
		}, res)
	}

	{ // Unwrap status.
		validate(t, func() bool { return !errors.Is(err, Terminated) }, err)
	}

	{ // Unwrap cause.
		validate(t, func() bool { return !errors.Is(err, io.EOF) }, err)
	}
}

func TestChain(t *testing.T) {
	{ // Successful chaining.
		var res, err = Chain(func(buf []byte) (int, error) {
			if !bytes.Equal(buf, []byte{1, 2, 3, 4, 5}) {
				return 2, fmt.Errorf("codec 1: received invalid buffer: %v", buf)
			}
			return 2, io.EOF
		}, func(buf []byte) (int, error) {
			if !bytes.Equal(buf, []byte{3, 4, 5}) {
				return 3, fmt.Errorf("codec 2: received invalid buffer: %v", buf)
			}
			return 3, io.EOF
		})([]byte{1, 2, 3, 4, 5})
		validate(t, func() bool {
			return res != 5 || !errors.Is(err, io.EOF)
		}, res, err)
	}

	{ // Failing chaining.
		var res, err = Chain(func(_ []byte) (int, error) {
			return 0, io.ErrNoProgress
		})([]byte{0})
		validate(t, func() bool {
			return res != 0 || !errors.Is(err, io.ErrNoProgress)
		}, res, err)
	}
}

func TestReader(t *testing.T) {
	{ // Default execution.
		var res, err = Reader(func(buf []byte) (int, error) {
			if !bytes.Equal(buf, []byte{1, 2, 3}) {
				return 3, fmt.Errorf("codec: received invalid buffer: %v", buf)
			}
			return 3, io.EOF
		}).Read([]byte{1, 2, 3})
		validate(t, func() bool {
			return res != 3 || !errors.Is(err, io.EOF)
		}, res, err)
	}
}

func TestWriter(t *testing.T) {
	{ // Default execution.
		var res, err = Writer(func(buf []byte) (int, error) {
			if !bytes.Equal(buf, []byte{1, 2, 3}) {
				return 3, fmt.Errorf("codec: received invalid buffer: %v", buf)
			}
			return 3, io.EOF
		}).Write([]byte{1, 2, 3})
		validate(t, func() bool {
			return res != 3 || !errors.Is(err, io.EOF)
		}, res, err)
	}
}

func TestExecutor(t *testing.T) {
	{ // Default execution.
		var err = Executor(func(cmd Handler) error {
			return cmd(nil, nil)
		}).Execute(func(_, _ Codec) error {
			return io.EOF
		})
		validate(t, func() bool {
			return !errors.Is(err, io.EOF)
		}, err)
	}
}

func TestNetwork(t *testing.T) {
	const endpoint = "localhost:11111"
	go func() {
		var l, err = net.Listen("tcp", endpoint)
		if err != nil {
			panic(err)
		}
		{
			var con, err = l.Accept()
			if err != nil {
				panic(err)
			}
			defer con.Close()
			if _, err := con.Write([]byte("Hello")); err != nil {
				panic(err)
			}
			var buf = make([]byte, 5)
			if _, err := con.Read(buf); err != nil {
				panic(err)
			}
			if !bytes.Equal(buf, []byte("World")) {
				panic(fmt.Sprintf("received: %v", string(buf)))
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err := l.Close(); err != nil {
			panic(err)
		}
	}()

	time.Sleep(1 * time.Second)

	var (
		ctx = cancel.New()
		con = (Network{
			Endpoint: endpoint,
			Protocol: "tcp",
			Watchdog: 100 * time.Millisecond,
		}).Client(ctx)
	)

	{ // Test normal connection.
		var err = con.Execute(func(rx, tx Codec) error {
			var buf = make([]byte, 6)
			if n, err := rx(buf); n != 5 || err != nil {
				return fmt.Errorf("rx: n: %v err: %v", n, err)
			}
			if n, err := tx([]byte("World")); n != 5 || err != nil {
				return fmt.Errorf("tx: n: %v err: %v", n, err)
			}
			return nil
		})
		validate(t, func() bool {
			return err != nil
		}, err)
	}

	{ // Frozen connection.
		var err = con.Execute(func(rx, _ Codec) error {
			var _, err = rx([]byte{1})
			return err
		})
		validate(t, func() bool {
			var err, ok = err.(net.Error)
			return !ok || !err.Timeout()
		}, err)
	}

	{ // Terminated connection.
		time.Sleep(500 * time.Millisecond)
		var err = con.Execute(func(rx, _ Codec) error {
			var _, err = rx([]byte{1})
			return err
		})
		validate(t, func() bool {
			return !errors.Is(err, io.EOF)
		}, err)
	}

	{ // Dial timeout.
		var err = con.Execute(func(rx, _ Codec) error {
			var _, err = rx([]byte{1})
			return err
		})
		validate(t, func() bool {
			return !errors.Is(err, Terminated)
		}, err)
	}

	{ // Canceled connection.
		ctx.Cancel()
		var err = con.Execute(func(rx, _ Codec) error {
			var _, err = rx([]byte{1})
			return err
		})
		validate(t, func() bool {
			return !errors.Is(err, Terminated) || !errors.Is(err, context.Canceled)
		}, err)
	}
}
