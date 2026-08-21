# task106

Go 实现的锁协调服务：提供租约锁、等待队列、限流、拓扑、交接、审计和告警等能力，状态统一持久化到 SQLite。

## 快速开始

```sh
export GOTOOLCHAIN=local
go test ./...
go vet ./...
go build ./...
go run ./cmd/lock-server --smoke-test
go run ./cmd/lock-server
```

服务默认监听 `:8080`，数据库默认使用 `./data/locks.db`。完整的容器构建和双架构说明见 [BENZHI_README.md](BENZHI_README.md)。

协调控制面位于 `/api/v1/coordination`，负责资源层级、维护窗口、fencing token 和重启恢复检查；锁申请会经过资源状态与策略准入。
