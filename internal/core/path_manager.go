package core

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/stream"
)

func pathConfCanBeUpdated(oldPathConf *conf.Path, newPathConf *conf.Path) bool {
	clone := oldPathConf.Clone()

	clone.Record = newPathConf.Record

	clone.RPICameraBrightness = newPathConf.RPICameraBrightness
	clone.RPICameraContrast = newPathConf.RPICameraContrast
	clone.RPICameraSaturation = newPathConf.RPICameraSaturation
	clone.RPICameraSharpness = newPathConf.RPICameraSharpness
	clone.RPICameraExposure = newPathConf.RPICameraExposure
	clone.RPICameraAWB = newPathConf.RPICameraAWB
	clone.RPICameraAWBGains = newPathConf.RPICameraAWBGains
	clone.RPICameraDenoise = newPathConf.RPICameraDenoise
	clone.RPICameraShutter = newPathConf.RPICameraShutter
	clone.RPICameraMetering = newPathConf.RPICameraMetering
	clone.RPICameraGain = newPathConf.RPICameraGain
	clone.RPICameraEV = newPathConf.RPICameraEV
	clone.RPICameraFPS = newPathConf.RPICameraFPS
	clone.RPICameraIDRPeriod = newPathConf.RPICameraIDRPeriod
	clone.RPICameraBitrate = newPathConf.RPICameraBitrate

	return newPathConf.Equal(clone)
}

type pathManagerHLSServer interface {
	PathReady(defs.Path)
	PathNotReady(defs.Path)
}

type pathManagerParent interface {
	logger.Writer
}

type pathManager struct {
	logLevel          conf.LogLevel
	authManager       *auth.Manager
	rtspAddress       string
	readTimeout       conf.StringDuration
	writeTimeout      conf.StringDuration
	writeQueueSize    int
	udpMaxPayloadSize int
	pathConfs         map[string]*conf.Path
	externalCmdPool   *externalcmd.Pool
	parent            pathManagerParent

	ctx         context.Context
	ctxCancel   func()
	wg          sync.WaitGroup
	hlsManager  pathManagerHLSServer
	paths       map[string]*path
	pathsByConf map[string]map[*path]struct{}

	// in
	chReloadConf   chan map[string]*conf.Path
	chSetHLSServer chan pathManagerHLSServer
	chClosePath    chan *path
	chPathReady    chan *path
	chPathNotReady chan *path
	chFindPathConf chan defs.PathFindPathConfReq
	chDescribe     chan defs.PathDescribeReq
	chAddReader    chan defs.PathAddReaderReq
	chAddPublisher chan defs.PathAddPublisherReq
	chAPIPathsList chan pathAPIPathsListReq
	chAPIPathsGet  chan pathAPIPathsGetReq
}

func (pm *pathManager) initialize() {
	ctx, ctxCancel := context.WithCancel(context.Background())

	pm.ctx = ctx
	pm.ctxCancel = ctxCancel
	pm.paths = make(map[string]*path)
	pm.pathsByConf = make(map[string]map[*path]struct{})
	pm.chReloadConf = make(chan map[string]*conf.Path)
	pm.chSetHLSServer = make(chan pathManagerHLSServer)
	pm.chClosePath = make(chan *path)
	pm.chPathReady = make(chan *path)
	pm.chPathNotReady = make(chan *path)
	pm.chFindPathConf = make(chan defs.PathFindPathConfReq)
	pm.chDescribe = make(chan defs.PathDescribeReq)
	pm.chAddReader = make(chan defs.PathAddReaderReq)
	pm.chAddPublisher = make(chan defs.PathAddPublisherReq)
	pm.chAPIPathsList = make(chan pathAPIPathsListReq)
	pm.chAPIPathsGet = make(chan pathAPIPathsGetReq)

	for pathConfName, pathConf := range pm.pathConfs {
		if pathConf.Regexp == nil {
			pm.createPath(pathConfName, pathConf, pathConfName, nil)
		}
	}

	pm.Log(logger.Debug, "path manager created")

	pm.wg.Add(1)
	go pm.run()
}

func (pm *pathManager) close() {
	pm.Log(logger.Debug, "path manager is shutting down")
	pm.ctxCancel()
	pm.wg.Wait()
}

// Log implements logger.Writer.
func (pm *pathManager) Log(level logger.Level, format string, args ...interface{}) {
	pm.parent.Log(level, format, args...)
}

func (pm *pathManager) run() {
	defer pm.wg.Done()

outer:
	for {
		select {
		case newPaths := <-pm.chReloadConf:
			pm.doReloadConf(newPaths)

		case m := <-pm.chSetHLSServer:
			pm.doSetHLSServer(m)

		case pa := <-pm.chClosePath:
			pm.doClosePath(pa)

		case pa := <-pm.chPathReady:
			pm.doPathReady(pa)

		case pa := <-pm.chPathNotReady:
			pm.doPathNotReady(pa)

		case req := <-pm.chFindPathConf:
			pm.doFindPathConf(req)

		case req := <-pm.chDescribe:
			pm.doDescribe(req)

		case req := <-pm.chAddReader:
			pm.doAddReader(req)

		case req := <-pm.chAddPublisher:
			pm.doAddPublisher(req)

		case req := <-pm.chAPIPathsList:
			pm.doAPIPathsList(req)

		case req := <-pm.chAPIPathsGet:
			pm.doAPIPathsGet(req)

		case <-pm.ctx.Done():
			break outer
		}
	}

	pm.ctxCancel()
}

func (pm *pathManager) doReloadConf(newPaths map[string]*conf.Path) {
	for confName, pathConf := range pm.pathConfs {
		if newPath, ok := newPaths[confName]; ok {
			// configuration has changed
			if !newPath.Equal(pathConf) {
				if pathConfCanBeUpdated(pathConf, newPath) { // paths associated with the configuration can be updated
					for pa := range pm.pathsByConf[confName] {
						go pa.reloadConf(newPath)
					}
				} else { // paths associated with the configuration must be recreated
					for pa := range pm.pathsByConf[confName] {
						pm.removePath(pa)
						pa.close()
						pa.wait() // avoid conflicts between sources
					}
				}
			}
		} else {
			// configuration has been deleted, remove associated paths
			for pa := range pm.pathsByConf[confName] {
				pm.removePath(pa)
				pa.close()
				pa.wait() // avoid conflicts between sources
			}
		}
	}

	pm.pathConfs = newPaths

	// add new paths
	for pathConfName, pathConf := range pm.pathConfs {
		if _, ok := pm.paths[pathConfName]; !ok && pathConf.Regexp == nil {
			pm.createPath(pathConfName, pathConf, pathConfName, nil)
		}
	}
}

func (pm *pathManager) doSetHLSServer(m pathManagerHLSServer) {
	pm.hlsManager = m
}

func (pm *pathManager) doClosePath(pa *path) {
	if pmpa, ok := pm.paths[pa.name]; !ok || pmpa != pa {
		return
	}
	pm.removePath(pa)
}

func (pm *pathManager) doPathReady(pa *path) {
	if pm.hlsManager != nil {
		pm.hlsManager.PathReady(pa)
	}
}

func (pm *pathManager) doPathNotReady(pa *path) {
	if pm.hlsManager != nil {
		pm.hlsManager.PathNotReady(pa)
	}
}

func (pm *pathManager) doFindPathConf(req defs.PathFindPathConfReq) {
	_, pathConf, _, err := conf.FindPathConf(pm.pathConfs, req.AccessRequest.Name)
	if err != nil {
		req.Res <- defs.PathFindPathConfRes{Err: err}
		return
	}

	/* 기존 내부인증
	err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
	if err != nil {
		req.Res <- defs.PathFindPathConfRes{Err: err}
		return
	}
	*/

	// 외부 Digest 인증
	// Authorization 헤더가 존재하는지 확인
	authHeaders, ok := (*req.AccessRequest.RTSPRequest).Header["Authorization"]
	if !ok || len(authHeaders) == 0 {
		err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
		req.Res <- defs.PathFindPathConfRes{Err: err}
		return
	}

	// Digest 인증 정보 추출
	authHeader := authHeaders[0] // 여기서는 첫 번째 Authorization 헤더만 사용
	Method := (*req.AccessRequest.RTSPRequest).Method
	// Django 인증 서버로 보낼 요청 생성
	client := &http.Client{}
	authReq, err := http.NewRequest(string(Method), "http://127.0.0.1:8000/auth", nil)
	if err != nil {
		// 요청 생성 중 오류 처리
		req.Res <- defs.PathFindPathConfRes{Err: err}
		return
	}
	authReq.Header.Add("Authorization", authHeader)

	// 요청 전송 및 응답 수신
	resp, err := client.Do(authReq)
	if err != nil {
		// 요청 전송 중 오류 처리
		req.Res <- defs.PathFindPathConfRes{Err: err}
		return
	}
	defer resp.Body.Close()

	// 응답 처리 예시 (실제 응답에 따라 수정 필요)
	// 여기서는 상태 코드가 200이 아닐 경우 에러로 간주
	if resp.StatusCode == http.StatusUnauthorized {
		err := fmt.Errorf("authentication failed with status code: %d", resp.StatusCode)
		req.Res <- defs.PathFindPathConfRes{Err: err}
		return
	}

	req.Res <- defs.PathFindPathConfRes{Conf: pathConf}
}

func (pm *pathManager) doDescribe(req defs.PathDescribeReq) {
	pathConfName, pathConf, pathMatches, err := conf.FindPathConf(pm.pathConfs, req.AccessRequest.Name)
	if err != nil {
		req.Res <- defs.PathDescribeRes{Err: err}
		return
	}
	// Digest 인증
	// Authorization 헤더가 존재하는지 확인
	authHeaders, ok := (*req.AccessRequest.RTSPRequest).Header["Authorization"]
	if !ok || len(authHeaders) == 0 {
		err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
		req.Res <- defs.PathDescribeRes{Err: err}
		return
	}

	// Digest 인증 정보 추출
	authHeader := authHeaders[0] // 여기서는 첫 번째 Authorization 헤더만 사용
	Method := (*req.AccessRequest.RTSPRequest).Method
	// Django 인증 서버로 보낼 요청 생성
	client := &http.Client{}
	authReq, err := http.NewRequest(string(Method), "http://127.0.0.1:8000/auth", nil)
	if err != nil {
		// 요청 생성 중 오류 처리
		req.Res <- defs.PathDescribeRes{Err: err}
		return
	}
	authReq.Header.Add("Authorization", authHeader)

	// 요청 전송 및 응답 수신
	resp, err := client.Do(authReq)
	if err != nil {
		// 요청 전송 중 오류 처리
		req.Res <- defs.PathDescribeRes{Err: err}
		return
	}
	defer resp.Body.Close()

	// 응답 처리 예시 (실제 응답에 따라 수정 필요)
	// 여기서는 상태 코드가 200이 아닐 경우 에러로 간주
	if resp.StatusCode == http.StatusUnauthorized {
		err := fmt.Errorf("authentication failed with status code: %d", resp.StatusCode)
		req.Res <- defs.PathDescribeRes{Err: err}
		return
	}

	// create path if it doesn't exist
	if _, ok := pm.paths[req.AccessRequest.Name]; !ok {
		pm.createPath(pathConfName, pathConf, req.AccessRequest.Name, pathMatches)
	}

	req.Res <- defs.PathDescribeRes{Path: pm.paths[req.AccessRequest.Name]}
}

func (pm *pathManager) doAddReader(req defs.PathAddReaderReq) {
	pathConfName, pathConf, pathMatches, err := conf.FindPathConf(pm.pathConfs, req.AccessRequest.Name)
	if err != nil {
		req.Res <- defs.PathAddReaderRes{Err: err}
		return
	}

	if !req.AccessRequest.SkipAuth {
		/*
			기존 내부 인증
			err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
			if err != nil {
				req.Res <- defs.PathAddReaderRes{Err: err}
				return
			}
		*/

		// 외부 Digest 인증
		// Authorization 헤더가 존재하는지 확인
		authHeaders, ok := (*req.AccessRequest.RTSPRequest).Header["Authorization"]
		if !ok || len(authHeaders) == 0 {
			err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
			req.Res <- defs.PathAddReaderRes{Err: err}
			return
		}

		// Digest 인증 정보 추출
		authHeader := authHeaders[0] // 여기서는 첫 번째 Authorization 헤더만 사용
		Method := (*req.AccessRequest.RTSPRequest).Method
		// Django 인증 서버로 보낼 요청 생성
		client := &http.Client{}
		authReq, err := http.NewRequest(string(Method), "http://127.0.0.1:8000/auth", nil)
		if err != nil {
			// 요청 생성 중 오류 처리
			req.Res <- defs.PathAddReaderRes{Err: err}
			return
		}
		authReq.Header.Add("Authorization", authHeader)

		// 요청 전송 및 응답 수신
		resp, err := client.Do(authReq)
		if err != nil {
			// 요청 전송 중 오류 처리
			req.Res <- defs.PathAddReaderRes{Err: err}
			return
		}
		defer resp.Body.Close()

		// 응답 처리 예시 (실제 응답에 따라 수정 필요)
		// 여기서는 상태 코드가 200이 아닐 경우 에러로 간주
		if resp.StatusCode == http.StatusUnauthorized {
			err := fmt.Errorf("authentication failed with status code: %d", resp.StatusCode)
			req.Res <- defs.PathAddReaderRes{Err: err}
			return
		}
	}

	// create path if it doesn't exist
	if _, ok := pm.paths[req.AccessRequest.Name]; !ok {
		pm.createPath(pathConfName, pathConf, req.AccessRequest.Name, pathMatches)
	}

	req.Res <- defs.PathAddReaderRes{Path: pm.paths[req.AccessRequest.Name]}
}

func (pm *pathManager) doAddPublisher(req defs.PathAddPublisherReq) {
	pathConfName, pathConf, pathMatches, err := conf.FindPathConf(pm.pathConfs, req.AccessRequest.Name)
	if err != nil {
		req.Res <- defs.PathAddPublisherRes{Err: err}
		return
	}

	if !req.AccessRequest.SkipAuth {
		/*
			기존 내부인증
				err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
				if err != nil {
					req.Res <- defs.PathAddPublisherRes{Err: err}
					return
				}
		*/

		// 외부 Digest 인증
		// Authorization 헤더가 존재하는지 확인
		authHeaders, ok := (*req.AccessRequest.RTSPRequest).Header["Authorization"]
		if !ok || len(authHeaders) == 0 {
			err = pm.authManager.Authenticate(req.AccessRequest.ToAuthRequest())
			req.Res <- defs.PathAddPublisherRes{Err: err}
			return
		}

		// Digest 인증 정보 추출
		authHeader := authHeaders[0] // 여기서는 첫 번째 Authorization 헤더만 사용
		Method := (*req.AccessRequest.RTSPRequest).Method
		// Django 인증 서버로 보낼 요청 생성
		client := &http.Client{}
		authReq, err := http.NewRequest(string(Method), "http://127.0.0.1:8000/auth", nil)
		if err != nil {
			// 요청 생성 중 오류 처리
			req.Res <- defs.PathAddPublisherRes{Err: err}
			return
		}
		authReq.Header.Add("Authorization", authHeader)

		// 요청 전송 및 응답 수신
		resp, err := client.Do(authReq)
		if err != nil {
			// 요청 전송 중 오류 처리
			req.Res <- defs.PathAddPublisherRes{Err: err}
			return
		}
		defer resp.Body.Close()

		// 응답 처리 예시 (실제 응답에 따라 수정 필요)
		// 여기서는 상태 코드가 200이 아닐 경우 에러로 간주
		if resp.StatusCode == http.StatusUnauthorized {
			err := fmt.Errorf("authentication failed with status code: %d", resp.StatusCode)
			req.Res <- defs.PathAddPublisherRes{Err: err}
			return
		}
	}

	// create path if it doesn't exist
	if _, ok := pm.paths[req.AccessRequest.Name]; !ok {
		pm.createPath(pathConfName, pathConf, req.AccessRequest.Name, pathMatches)
	}

	req.Res <- defs.PathAddPublisherRes{Path: pm.paths[req.AccessRequest.Name]}
}

func (pm *pathManager) doAPIPathsList(req pathAPIPathsListReq) {
	paths := make(map[string]*path)

	for name, pa := range pm.paths {
		paths[name] = pa
	}

	req.res <- pathAPIPathsListRes{paths: paths}
}

func (pm *pathManager) doAPIPathsGet(req pathAPIPathsGetReq) {
	path, ok := pm.paths[req.name]
	if !ok {
		req.res <- pathAPIPathsGetRes{err: conf.ErrPathNotFound}
		return
	}

	req.res <- pathAPIPathsGetRes{path: path}
}

func (pm *pathManager) createPath(
	pathConfName string,
	pathConf *conf.Path,
	name string,
	matches []string,
) {
	pa := &path{
		parentCtx:         pm.ctx,
		logLevel:          pm.logLevel,
		rtspAddress:       pm.rtspAddress,
		readTimeout:       pm.readTimeout,
		writeTimeout:      pm.writeTimeout,
		writeQueueSize:    pm.writeQueueSize,
		udpMaxPayloadSize: pm.udpMaxPayloadSize,
		confName:          pathConfName,
		conf:              pathConf,
		name:              name,
		matches:           matches,
		wg:                &pm.wg,
		externalCmdPool:   pm.externalCmdPool,
		parent:            pm,
	}
	pa.initialize()

	pm.paths[name] = pa

	if _, ok := pm.pathsByConf[pathConfName]; !ok {
		pm.pathsByConf[pathConfName] = make(map[*path]struct{})
	}
	pm.pathsByConf[pathConfName][pa] = struct{}{}
}

func (pm *pathManager) removePath(pa *path) {
	delete(pm.pathsByConf[pa.confName], pa)
	if len(pm.pathsByConf[pa.confName]) == 0 {
		delete(pm.pathsByConf, pa.confName)
	}
	delete(pm.paths, pa.name)
}

// ReloadPathConfs is called by core.
func (pm *pathManager) ReloadPathConfs(pathConfs map[string]*conf.Path) {
	select {
	case pm.chReloadConf <- pathConfs:
	case <-pm.ctx.Done():
	}
}

// pathReady is called by path.
func (pm *pathManager) pathReady(pa *path) {
	select {
	case pm.chPathReady <- pa:
	case <-pm.ctx.Done():
	case <-pa.ctx.Done(): // in case pathManager is blocked by path.wait()
	}
}

// pathNotReady is called by path.
func (pm *pathManager) pathNotReady(pa *path) {
	select {
	case pm.chPathNotReady <- pa:
	case <-pm.ctx.Done():
	case <-pa.ctx.Done(): // in case pathManager is blocked by path.wait()
	}
}

// closePath is called by path.
func (pm *pathManager) closePath(pa *path) {
	select {
	case pm.chClosePath <- pa:
	case <-pm.ctx.Done():
	case <-pa.ctx.Done(): // in case pathManager is blocked by path.wait()
	}
}

// GetConfForPath is called by a reader or publisher.
func (pm *pathManager) FindPathConf(req defs.PathFindPathConfReq) (*conf.Path, error) {
	req.Res = make(chan defs.PathFindPathConfRes)
	select {
	case pm.chFindPathConf <- req:
		res := <-req.Res
		return res.Conf, res.Err

	case <-pm.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// Describe is called by a reader or publisher.
func (pm *pathManager) Describe(req defs.PathDescribeReq) defs.PathDescribeRes {
	req.Res = make(chan defs.PathDescribeRes)
	select {
	case pm.chDescribe <- req:
		res1 := <-req.Res
		if res1.Err != nil {
			return res1
		}

		res2 := res1.Path.(*path).describe(req)
		if res2.Err != nil {
			return res2
		}

		res2.Path = res1.Path
		return res2

	case <-pm.ctx.Done():
		return defs.PathDescribeRes{Err: fmt.Errorf("terminated")}
	}
}

// AddPublisher is called by a publisher.
func (pm *pathManager) AddPublisher(req defs.PathAddPublisherReq) (defs.Path, error) {
	req.Res = make(chan defs.PathAddPublisherRes)
	select {
	case pm.chAddPublisher <- req:
		res := <-req.Res
		if res.Err != nil {
			return nil, res.Err
		}

		return res.Path.(*path).addPublisher(req)

	case <-pm.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// AddReader is called by a reader.
func (pm *pathManager) AddReader(req defs.PathAddReaderReq) (defs.Path, *stream.Stream, error) {
	req.Res = make(chan defs.PathAddReaderRes)
	select {
	case pm.chAddReader <- req:
		res := <-req.Res
		if res.Err != nil {
			return nil, nil, res.Err
		}

		return res.Path.(*path).addReader(req)

	case <-pm.ctx.Done():
		return nil, nil, fmt.Errorf("terminated")
	}
}

// setHLSServer is called by hlsManager.
func (pm *pathManager) setHLSServer(s pathManagerHLSServer) {
	select {
	case pm.chSetHLSServer <- s:
	case <-pm.ctx.Done():
	}
}

// APIPathsList is called by api.
func (pm *pathManager) APIPathsList() (*defs.APIPathList, error) {
	req := pathAPIPathsListReq{
		res: make(chan pathAPIPathsListRes),
	}

	select {
	case pm.chAPIPathsList <- req:
		res := <-req.res

		res.data = &defs.APIPathList{
			Items: []*defs.APIPath{},
		}

		for _, pa := range res.paths {
			item, err := pa.APIPathsGet(pathAPIPathsGetReq{})
			if err == nil {
				res.data.Items = append(res.data.Items, item)
			}
		}

		sort.Slice(res.data.Items, func(i, j int) bool {
			return res.data.Items[i].Name < res.data.Items[j].Name
		})

		return res.data, nil

	case <-pm.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// APIPathsGet is called by api.
func (pm *pathManager) APIPathsGet(name string) (*defs.APIPath, error) {
	req := pathAPIPathsGetReq{
		name: name,
		res:  make(chan pathAPIPathsGetRes),
	}

	select {
	case pm.chAPIPathsGet <- req:
		res := <-req.res
		if res.err != nil {
			return nil, res.err
		}

		data, err := res.path.APIPathsGet(req)
		return data, err

	case <-pm.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}
