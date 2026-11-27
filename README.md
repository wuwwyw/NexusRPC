# NexusIM RPC

NexusIM RPC 是一个面向即时通信系统内部微服务的 C++17 RPC 工具。客户端通过 etcd 发现服务实例，通过 TCP 发送请求并接收响应。服务端采用基于 Linux `epoll` 的多 Reactor 模型，消息使用 Protobuf 编码。

## 已实现

- **请求与响应**：TCP 长连接、自定义帧头、分包与粘包处理，以及通过 `request_id` 匹配响应。
- **服务端**：主 Reactor 接收新连接并按轮询分发，子 Reactor 各自使用 `epoll` 管理分配到的连接。工作线程执行处理函数，完成后通过 `eventfd` 通知连接所属的子 Reactor 发送响应。支持读暂停与恢复，以及 TCP 半关闭后的响应发送。
- **客户端**：同步 `Call()`、按实例地址预建和复用连接、借出时检查连接，以及失败后的重连。
- **服务发现**：etcd 快照与 Watch 衔接、地址校验、实例下线回收空闲连接，以及租约失效后的重新注册。

当前同一个 `Rpc_user` 实例按串行调用使用。RPC 正文上限为 1 MiB，包含序列化后的请求或响应字段。已经开始发送的请求遇到超时或断线时，由调用方决定是否重试。

服务端默认启动 4 个子 Reactor，数量由 [`common/Config.hpp`](common/Config.hpp) 中的 `reactor_num` 控制，修改后需重新编译。调用 `request_stop()` 可唤醒主循环并退出 `loop()`。销毁服务端时会唤醒并等待各子 Reactor 退出，尚未发出的响应可能被丢弃。

## 目录

| 路径 | 内容 |
| --- | --- |
| [`NexusRPC/`](NexusRPC/) | RPC 服务端、子 Reactor（`Rpc_loop.hpp`）、客户端、协议、连接池和服务表 |
| [`NexusRPC/proto/`](NexusRPC/proto/) | Protobuf 定义及生成的 C++ 代码 |
| [`Tcp_IO/`](Tcp_IO/) | TCP socket、地址、`epoll` 和非阻塞收发 |
| [`common/`](common/) | etcd 注册与发现、工作线程池、日志和配置 |
| [`examples/echo/`](examples/echo/) | 可独立运行的 Echo 服务端和客户端 |

## 环境和依赖

- Linux，已在 WSL Ubuntu 22.04 上验证。网络层依赖 Linux 的 `epoll`。
- CMake 3.16 或以上，以及支持 C++17 的编译器，例如 `g++`。
- Protobuf C++、etcd-cpp-apiv3、cpprestsdk、spdlog 和 fmt 的开发头文件及库。CMake 需要能找到 `Protobuf`、`etcd-cpp-api` 和 `spdlog`，并解析它们的依赖。
- 可连接的 etcd 实例。默认地址为 `http://127.0.0.1:2379`，注册前缀为 `/NexusiM/test/`，都在 [`common/Config.hpp`](common/Config.hpp) 中配置。

## 运行 Echo 示例

[服务端](examples/echo/echo_server.cpp)通过 `register_method()` 注册 `service/echo`，原样返回请求正文。[客户端](examples/echo/echo_client.cpp)通过 etcd 找到服务端，发送 `hello` 并检查响应。

**1. 编译动态库和示例。** 在仓库根目录执行：

```bash
cmake -S . -B build -DNEXUS_RPC_BUILD_EXAMPLES=ON
cmake --build build --parallel
```

构建完成后，动态库位于 `build/libnexus_rpc.so`，示例程序位于 `build/examples/echo/`。

**2. 启动 etcd。** 如果 `127.0.0.1:2379` 上还没有 etcd，在单独的终端执行以下命令，并保持它运行：

```bash
etcd --data-dir "$(mktemp -d)"
```

**3. 启动服务端。** 在仓库根目录执行，保持服务端运行：

```bash
./build/examples/echo/echo_server
```

**4. 运行客户端。** 在另一个终端进入仓库根目录后执行：

```bash
./build/examples/echo/echo_client
```

客户端应输出 `hello`。

接入自己的处理函数时，先调用 `server.register_method(service, method, handler)`，再调用 `server.loop()` 启动主事件循环。etcd 注册 key 使用 `<prefix><service>/<instance>`，其中的服务名要和 `register_method()`、`Call()` 使用的服务名一致。注册地址使用客户端可达的 `IPv4:端口`。

## 开发状态

当前版本面向可信内网中的 Linux C++ 服务间调用。独立 Echo 示例、多 Reactor 连接分流和停机流程已在 WSL 验证。
