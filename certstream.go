package certstream

import (
    "context"
    "io"
    "strconv"
    "sync"
    "time"

    "github.com/gobwas/ws"
    "github.com/gobwas/ws/wsutil"
    "github.com/paulbellamy/ratecounter"
    "github.com/pkg/errors"
    "github.com/valyala/fastjson"
    "go.uber.org/zap"
)

const (
    addr = "wss://certstream.calidog.io"
)

const (
    // Send pings to certstream server with this period to keep conn alive
    pingPeriod = 15 * time.Second
)

var (
    p fastjson.Parser
    logger, _ = zap.NewDevelopment()
    sugar = logger.Sugar()
)

func CertStreamEventStream(debug bool) (chan *fastjson.Value, chan error) {
    outputStream := make(chan *fastjson.Value)
    errStream := make(chan error)

    go func() {
        for {
            cl, err := newClient(debug)

            if err != nil {
                errStream <- errors.Wrap(err, "error connecting to certstream, sleeping a few seconds and reconnecting...")
                time.Sleep(5 * time.Second)
                continue
            }
            if debug {
                sugar.Info("connected to certstream")
            }

            done := make(chan struct{})
            go cl.startPing(done)

            loop:
            for {
                messages, err := cl.receive()
                if err != nil {
                    if debug {
                        errStream <- err
                    }
                    break loop
                }
                if len(messages) == 0 {
                    continue
                }

                for _, msg := range messages {
                    switch msg.OpCode {
                    case ws.OpText:
                        v, err := p.Parse(string(msg.Payload))
                        if err != nil {
                            break
                        }
                        if string(v.GetStringBytes("message_type")) == "certificate_update" {
                            outputStream <- v
                        }
                    case ws.OpPong:
                        if debug {
                            sugar.Info("pong")
                        }
                    default:
                    }
                }
            }


            close(done)
            cl.conn.Close()
        }
    }()

    return outputStream, errStream
}

type client struct {
    io sync.Mutex

    conn io.ReadWriteCloser
    rc *ratecounter.RateCounter
    debug bool
}

func newClient(debug bool) (*client, error) {
    ctx := context.Background()
    conn, _, _, err := ws.Dial(ctx, addr)
    if err != nil {
        return nil, err
    }
    rc := ratecounter.NewRateCounter(1 * time.Second)
    cl := &client{
        conn: conn,
        rc: rc,
        debug: debug,
    }

    return cl, nil
}

func (cl *client) receive() ([]wsutil.Message, error) {
    cl.io.Lock()
    // r, err := wsutil.ReadServerText(cl.conn)
    messages, err := wsutil.ReadServerMessage(cl.conn, nil)
    cl.io.Unlock()
    if err != nil {
        return nil, err
    }

    cl.rc.Incr(1)

    return messages, nil
}

func (cl *client) startPing(done <-chan struct{}) {
    ticker := time.NewTicker(pingPeriod)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            cl.io.Lock()
            err := wsutil.WriteClientMessage(cl.conn, ws.OpPing, []byte{})
            cl.io.Unlock()
            if err != nil {
                ticker.Reset(pingPeriod)
                break
            }
            if cl.debug {
                cr := cl.rc.Rate()
                sugar.Infof("ping @ %s req/s", strconv.FormatInt(cr, 10))
            }
        case <-done:
            return
        }
    }
}