# task106 lock coordination service

这是一个基于 Go 的锁、配额和调用方协调服务。它把锁租约、等待队列、限流、拓扑关系、交接、审计和告警等状态保存到 SQLite，并通过资源控制面提供资源层级、维护隔离、fencing token 和重启恢复检查。

## 标准命令

```sh
export GOTOOLCHAIN=local
go test ./...
go vet ./...
go build ./...
go run ./cmd/lock-server
```

服务默认监听 `:8080`，数据库默认写入 `./data/locks.db`；可以通过 `ADDR` 和 `DB_PATH` 覆盖。

锁申请会经过 `/api/v1/coordination/resources` 的资源状态和策略检查；`/api/v1/coordination/fencing` 提供 token 签发/校验；`/api/v1/coordination/recovery` 提供启动和人工恢复 checkpoint。

## 自检

`--smoke-test` 不启动网络服务，也不依赖外部组件。它会创建临时 SQLite 数据库、写入锁状态、关闭并重新打开数据库，验证重启后的持久化恢复：

```sh
go run ./cmd/lock-server --smoke-test
```

## Docker

```sh
./build_benzhi_docker.sh task106 linux/amd64
docker run --rm task106 go run ./cmd/lock-server --smoke-test
```

`build_benzhi_docker.sh` 接受镜像名和平台两个参数，`benzhi.Dockerfile` 固定使用 Go 1.26.3；容器默认进入 Bash，便于执行构建、测试和自检命令。项目交付要求分别验证 `linux/amd64` 与 `linux/arm64`。
