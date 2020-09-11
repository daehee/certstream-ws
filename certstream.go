package certstream

import (
    "context"
    "io"
    "sync"
    "time"

    "github.com/gobwas/ws"
    "github.com/gobwas/ws/wsutil"
    "github.com/pkg/errors"
    "github.com/valyala/fastjson"
    "go.uber.org/zap"
    "go.uber.org/ratelimit"
)

const (
    addr = "wss://certstream.calidog.io"
)

const (
    // Send pings to certstream server with this period to keep conn alive
    pingPeriod = 15 * time.Second
    // requests per second limit to certstream sever
    rpsLimit = 1
)

var (
    p fastjson.Parser
    logger, _ = zap.NewDevelopment()
    sugar = logger.Sugar()
)

func CertStreamEventStream() (chan *fastjson.Value, chan error) {
    outputStream := make(chan *fastjson.Value)
    errStream := make(chan error)

    // rate limit the read requests to certstream server
    rl := ratelimit.New(rpsLimit)
    // prev := time.Now()

    go func() {
        for {
            cl, err := newClient()
            if err != nil {
                errStream <- errors.Wrap(err, "error connecting to certstream, sleeping a few seconds and reconnecting...")
                time.Sleep(5 * time.Second)
                continue
            }
            sugar.Info("connected to certstream")

            done := make(chan struct{})
            go cl.startPing(done)

            loop:
            for {
                _ = rl.Take()
                // sugar.Info(now.Sub(prev))
                // prev = now

                messages, err := cl.receive()
                if err != nil {
                    switch err {
                    case io.ErrUnexpectedEOF:
                        // silent reconnect on EOF from certstream server
                        break loop
                    default:
                        errStream <- err
                        break loop
                    }
                }
                if len(messages) == 0 {
                    continue
                }
                // var v *fastjson.Value
                for _, msg := range messages {
                    switch msg.OpCode {
                    case ws.OpText:
                        v, err := p.Parse(string(msg.Payload))
                        if err != nil {
                            errStream <- err
                        }
                        if string(v.GetStringBytes("message_type")) == "certificate_update" {
                            outputStream <- v
                        }
                    case ws.OpPong:
                        sugar.Info("pong")
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
}

func newClient() (*client, error) {
    ctx := context.Background()
    conn, _, _, err := ws.Dial(ctx, addr)
    if err != nil {
        return nil, err
    }
    cl := &client{
        conn: conn,
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
            sugar.Info("ping")
        case <-done:
            return
        }
    }
}