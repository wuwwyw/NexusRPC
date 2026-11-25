# NexusIM RPC

NexusIM RPC 是一个面向即时通信系统内部微服务的 C++17 RPC 工具。客户端通过 etcd 发现服务实例，再通过 TCP 发送请求并接收响应。服务端使用 Linux `epoll` 管理连接，由工作线程执行处理函数。消息使用 Protobuf 编码。

## 已实现

- **请求与响应**：TCP 长连接、自定义帧头、分包与粘包处理，以及通过 `request_id` 匹配响应。
- **服务端**：处理函数注册、工作线程执行、按连接管理收发状态。慢处理函数不会占住整个网络事件循环。
- **客户端**：同步 `Call()`、按实例地址预建和复用连接、借出时检查连接，以及失败后的重连。
- **服务发现**：etcd 快照与 Watch 衔接、地址校验、实例下线回收空闲连接，以及租约失效后的重新注册。

当前同一个 `Rpc_user` 实例按串行调用使用。RPC 正文上限为 1 MiB，包含序列化后的请求或响应字段。已经开始发送的请求遇到超时或断线时，由调用方决定是否重试。

## 目录

| 路径 | 内容 |
| --- | --- |
| [`NexusRPC/`](NexusRPC/) | RPC 服务端、客户端、协议、连接池和服务表 |
| [`NexusRPC/proto/`](NexusRPC/proto/) | Protobuf 定义及生成的 C++ 代码 |
| [`Tcp_IO/`](Tcp_IO/) | TCP socket、地址、`epoll` 和非阻塞收发 |
| [`common/`](common/) | etcd 注册与发现、工作线程池、日志和配置 |

## 环境和依赖

- Linux，已在 WSL Ubuntu 22.04 上验证。网络层依赖 Linux 的 `epoll`。
- 支持 C++17 的编译器，例如 `g++`。
- Protobuf C++、etcd-cpp-apiv3、cpprestsdk、spdlog 和 fmt 的开发头文件及库。下方命令假设编译器和链接器能从系统路径找到它们；etcd-cpp-apiv3 的链接库名是 `etcd-cpp-api`。
- 可连接的 etcd 实例。默认地址为 `http://127.0.0.1:2379`，注册前缀为 `/NexusiM/test/`，都在 [`common/Config.hpp`](common/Config.hpp) 中配置。

如果本地还没启动 etcd，可以在一个 WSL 终端用临时数据目录启动它：

```bash
etcd --data-dir "$(mktemp -d)"
```

## 最小示例

下面的示例使用服务端内置的 `service/test` 方法，它会原样返回请求正文。分别将以下两段保存为 `server.cpp` 和 `client.cpp`。

`server.cpp`

```cpp
#include "NexusRPC/Rpc_server.hpp"

int main() {
    Rpc_server server("127.0.0.1", 19090, prefix + "service/node-19090");
    server.run();
}
```

`client.cpp`

```cpp
#include "NexusRPC/Rpc_uesr.hpp"
#include <iostream>

int main() {
    Rpc_user client;
    nexus_rpc::Rpc_response response;
    Rpc_error error = Rpc_error::NONE;

    if (!client.Call("service", "test", "hello RPC", response, 3000, &error)) {
        std::cerr << "RPC failed: " << static_cast<int>(error) << '\n';
        return 1;
    }
    if (response.status() != 0) {
        std::cerr << response.error_message() << '\n';
        return 2;
    }
    std::cout << response.payload() << '\n';
}
```

在仓库根目录执行以下编译命令。如果把示例放在其他目录，将命令中的 `server.cpp` 和 `client.cpp` 换成对应路径。

```bash
g++ -std=c++17 -pthread -I. server.cpp NexusRPC/proto/rpc.pb.cc \
    -letcd-cpp-api -lcpprest -lspdlog -lfmt -lprotobuf -o rpc-demo-server

g++ -std=c++17 -pthread -I. client.cpp NexusRPC/proto/rpc.pb.cc \
    -letcd-cpp-api -lcpprest -lspdlog -lfmt -lprotobuf -o rpc-demo-client
```

在 etcd 启动后，先运行 `./rpc-demo-server`，再在另一个终端运行 `./rpc-demo-client`。预期输出 `hello RPC`。

接入自己的处理函数时，在启动事件循环前调用 `server.register_method(service, method, handler)`。etcd 注册 key 使用 `<prefix><service>/<instance>`，其中的服务名要和 `register_method()`、`Call()` 使用的服务名一致。注册地址使用客户端可达的 `IPv4:端口`。

## 开发状态

目前面向可信内网中的 Linux C++ 服务间调用。基础收发、连接复用、实例上下线以及 etcd 重启恢复已在 WSL 验证。连接容量、写侧积压和多实例负载下的表现留待压测。
