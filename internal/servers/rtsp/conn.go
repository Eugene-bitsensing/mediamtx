package rtsp

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	rtspauth "github.com/bluenviron/gortsplib/v4/pkg/auth"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/headers"
	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/hooks"
	"github.com/bluenviron/mediamtx/internal/logger"
)

const (
	rtspAuthRealm = "atm220"
)

type conn struct {
	isTLS               bool
	rtspAddress         string
	authMethods         []headers.AuthMethod
	readTimeout         conf.StringDuration
	runOnConnect        string
	runOnConnectRestart bool
	runOnDisconnect     string
	externalCmdPool     *externalcmd.Pool
	pathManager         serverPathManager
	rconn               *gortsplib.ServerConn
	rserver             *gortsplib.Server
	parent              *Server

	uuid             uuid.UUID
	created          time.Time
	onDisconnectHook func()
	authNonce        string
	authFailures     int
}

func (c *conn) initialize() {
	c.uuid = uuid.New()
	c.created = time.Now()

	c.Log(logger.Info, "opened")

	desc := defs.APIPathSourceOrReader{
		Type: func() string {
			if c.isTLS {
				return "rtspsConn"
			}
			return "conn"
		}(),
		ID: c.uuid.String(),
	}

	c.onDisconnectHook = hooks.OnConnect(hooks.OnConnectParams{
		Logger:              c,
		ExternalCmdPool:     c.externalCmdPool,
		RunOnConnect:        c.runOnConnect,
		RunOnConnectRestart: c.runOnConnectRestart,
		RunOnDisconnect:     c.runOnDisconnect,
		RTSPAddress:         c.rtspAddress,
		Desc:                desc,
	})
}

// Log implements logger.Writer.
func (c *conn) Log(level logger.Level, format string, args ...interface{}) {
	c.parent.Log(level, "[conn %v] "+format, append([]interface{}{c.rconn.NetConn().RemoteAddr()}, args...)...)
}

// Conn returns the RTSP connection.
func (c *conn) Conn() *gortsplib.ServerConn {
	return c.rconn
}

func (c *conn) remoteAddr() net.Addr {
	return c.rconn.NetConn().RemoteAddr()
}

func (c *conn) ip() net.IP {
	return c.rconn.NetConn().RemoteAddr().(*net.TCPAddr).IP
}

// onClose is called by rtspServer.
func (c *conn) onClose(err error) {
	c.Log(logger.Info, "closed: %v", err)

	c.onDisconnectHook()
}

// onRequest is called by rtspServer.
func (c *conn) onRequest(req *base.Request) {
	c.Log(logger.Debug, "[c->s] %v", req)
}

// OnResponse is called by rtspServer.
func (c *conn) OnResponse(res *base.Response) {
	c.Log(logger.Debug, "[s->c] %v", res)
}

// onDescribe is called by rtspServer.
func (c *conn) onDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx,
) (*base.Response, *gortsplib.ServerStream, error) {
	if len(ctx.Path) == 0 || ctx.Path[0] != '/' {
		return &base.Response{
			StatusCode: base.StatusBadRequest,
		}, nil, fmt.Errorf("invalid path")
	}
	ctx.Path = ctx.Path[1:]

	if c.authNonce == "" {
		var err error
		c.authNonce, err = rtspauth.GenerateNonce()
		if err != nil {
			return &base.Response{
				StatusCode: base.StatusInternalServerError,
			}, nil, err
		}
	}

	res := c.pathManager.Describe(defs.PathDescribeReq{
		AccessRequest: defs.PathAccessRequest{
			Name:        ctx.Path,
			Query:       ctx.Query,
			IP:          c.ip(),
			Proto:       auth.ProtocolRTSP,
			ID:          &c.uuid,
			RTSPRequest: ctx.Request,
			RTSPNonce:   c.authNonce,
		},
	})

	if res.Err != nil {
		var terr auth.Error
		if errors.As(res.Err, &terr) {
			res, err := c.handleAuthError(terr)
			return res, nil, err
		}

		var terr2 defs.PathNoOnePublishingError
		if errors.As(res.Err, &terr2) {
			return &base.Response{
				StatusCode: base.StatusNotFound,
			}, nil, res.Err
		}

		return &base.Response{
			StatusCode: base.StatusBadRequest,
		}, nil, res.Err
	}

	if res.Redirect != "" {
		return &base.Response{
			StatusCode: base.StatusMovedPermanently,
			Header: base.Header{
				"Location": base.HeaderValue{res.Redirect},
			},
		}, nil, nil
	}

	var stream *gortsplib.ServerStream
	if !c.isTLS {
		stream = res.Stream.RTSPStream(c.rserver)
	} else {
		stream = res.Stream.RTSPSStream(c.rserver)
	}

	return &base.Response{
		StatusCode: base.StatusOK,
	}, stream, nil
}

func (c *conn) handleAuthError(authErr error) (*base.Response, error) {
	c.authFailures++

	// VLC with login prompt sends 4 requests:
	// 1) without credentials
	// 2) with password but without username
	// 3) without credentials
	// 4) with password and username
	// therefore we must allow up to 3 failures

	c.authMethods[0] = 2
	if c.authFailures <= 3 {
		return &base.Response{
			StatusCode: base.StatusUnauthorized,
			Header: base.Header{
				"WWW-Authenticate": GenerateWWWAuthenticate2(c.authMethods, rtspAuthRealm, c.authNonce),
				//"WWW-Authenticate2": rtspauth.GenerateWWWAuthenticate(c.authMethods, rtspAuthRealm, c.authNonce),
			},
		}, nil
	}

	// wait some seconds to mitigate brute force attacks
	<-time.After(auth.PauseAfterError)

	return &base.Response{
		StatusCode: base.StatusUnauthorized,
	}, authErr
}

func (c *conn) apiItem() *defs.APIRTSPConn {
	return &defs.APIRTSPConn{
		ID:            c.uuid,
		Created:       c.created,
		RemoteAddr:    c.remoteAddr().String(),
		BytesReceived: c.rconn.BytesReceived(),
		BytesSent:     c.rconn.BytesSent(),
	}
}

func GenerateWWWAuthenticate2(methods []headers.AuthMethod, realm string, nonce string) base.HeaderValue {
	// Django 서버의 /auth 엔드포인트로 GET 요청을 생성
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://127.0.0.1:8000/auth", nil)
	if err != nil {
		// 요청 생성 실패 처리
		panic(err) // 실제 코드에서는 panic 대신 적절한 에러 처리를 해야 합니다.
	}

	// 요청 전송 및 응답 수신
	resp, err := client.Do(req)
	if err != nil {
		// 요청 실패 처리
		panic(err) // 실제 코드에서는 panic 대신 적절한 에러 처리를 해야 합니다.
	}
	defer resp.Body.Close()

	// 응답 헤더에서 'Www-Authenticate' 값을 추출
	wwwAuthenticateHeaders, ok := resp.Header["Www-Authenticate"]
	if !ok || len(wwwAuthenticateHeaders) == 0 {
		// 'Www-Authenticate' 헤더가 없는 경우 처리
		panic("Www-Authenticate header is missing") // 실제 코드에서는 panic 대신 적절한 에러 처리를 해야 합니다.
	}

	// 추출한 'Www-Authenticate' 헤더 값을 HeaderValue 타입 변수에 저장
	ret := make(base.HeaderValue, 1)
	ret[0] = wwwAuthenticateHeaders[0] // 첫 번째 'Www-Authenticate' 헤더 값만 사용

	return ret
}
