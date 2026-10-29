package cli

import (
	"context"
	"io"
	"os"
)

// DesktopService 将桌面窗口与它启动的服务关联；附加已有服务时不会停止该服务。
type DesktopService struct {
	// cancel 仅取消当前桌面入口创建的服务运行上下文。
	cancel context.CancelFunc
	// done 在启动流程退出且所有服务资源清理完成后关闭。
	done chan struct{}
	// err 在 done 关闭前记录服务运行结果，调用方等待 done 后读取。
	err error
}

// StartDesktopService 复用 CLI 的配置、令牌、运行时和关闭流程，等待本地服务就绪。
func StartDesktopService(ctx context.Context, version string) (*DesktopService, error) {
	serviceContext, cancel := context.WithCancel(ctx)
	service := &DesktopService{cancel: cancel, done: make(chan struct{})}
	ready := make(chan struct{})
	opts := &runtimeOptions{
		Version:      version,
		Stdin:        os.Stdin,
		Stdout:       io.Discard,
		Stderr:       os.Stderr,
		Output:       string(outputHuman),
		serviceReady: func() { close(ready) },
	}
	go func() {
		defer close(service.done)
		service.err = runServe(serviceContext, opts, false, "")
	}()

	select {
	case <-ready:
		return service, nil
	case <-service.done:
		if service.err != nil {
			cancel()
			return nil, service.err
		}
		return service, nil
	case <-ctx.Done():
		_ = service.Close()
		return nil, ctx.Err()
	}
}

// Close 等待本入口启动的服务释放 TCP/MQTT 连接及运行信息，可重复调用。
func (s *DesktopService) Close() error {
	s.cancel()
	<-s.done
	return s.err
}
