#pragma once
#include "Rpc_uesr.hpp"
#include "Rpc_protocol.hpp"
#include "Rpc_receiver.hpp"
#include "Rpc_loop.hpp"

class Rpc_server {
public:
    using iter = std::unordered_map<int, std::shared_ptr<Rpc_receiver>>::iterator;
    using handler = std::function<void(const std::string&, nexus_rpc::Rpc_response&)>;

    Rpc_server(const std::string &ip, uint64_t port, const std::string &key) {
        _socket.create_sockfd();
        _socket.BindAndListen(ip, port);

        if ((_stop_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
            LOG_CRIT("stop_eventfd create fail");
            exit(1);
        }

        if (!_epoll.add(_socket.get_fd(), EPOLLIN)) {
            LOG_CRIT("server sockfd join epoll failed, fd = {}", _socket.get_fd());
            exit(1);
        }

        if (!_epoll.add(_stop_eventfd, EPOLLIN)) {
            LOG_CRIT("stop_eventfd join epoll failed, fd = {}", _stop_eventfd);
            exit(1);
        }

        _loop_eventfds.resize(reactor_num);
        _conn_fds.resize(reactor_num);

        for (int i = 0; i < reactor_num; i++) {
            _loop_eventfds[i] = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            auto loop = std::make_unique<Rpc_loop>(_loop_eventfds[i], _conn_fds[i], _conn_mutexs[i], _methods, _thread_pool);
            auto *ptr = loop.get();
            _rpc_loops.push_back(std::move(loop));
            _loop_threads.emplace_back([ptr] () {
                ptr->run();
            });
        }

        if (!create_service(ip, port, key)) {
            LOG_CRIT("create service failed");
            exit(1);
        }
        
        create_methods();
    }

    ~Rpc_server() {
        for (int i = 0; i < reactor_num; i++) {
            _rpc_loops[i]->Stop();
            _loop_threads[i].join();
            close(_loop_eventfds[i]);
        }
        close(_stop_eventfd);
    }

    void loop() {
        int idx = 0;
        while(true) {
            std::vector <epoll_event> events(event_size);
            int n = _epoll.wait(events);

            for (int i = 0; i < n; i++) {
                if (events[i].data.fd == _stop_eventfd) {
                    return ;
                }

                while (true) {
                    Inetaddr client_addr;
                    int client_fd = Accept(client_addr);
                    if(client_fd < 0) {
                        if (errno == EINTR || errno == ECONNABORTED) continue;
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            LOG_ERROR("accept failed: {}", strerror(errno));
                        }
                        break;
                    }

                    {
                        std::lock_guard<std::mutex> lock(_conn_mutexs[idx]);
                        _conn_fds[idx].push_back(client_fd);
                    }

                    uint64_t val = 1;
                    ssize_t notified;
                    do {
                        notified = write(_loop_eventfds[idx], &val, sizeof(val));
                    } while(notified < 0 && errno == EINTR);

                    if (notified < 0 && errno != EAGAIN) {
                        LOG_ERROR("connection notify failed: {}", strerror(errno));
                    }
                    idx = (idx + 1)%reactor_num;
                }
            }
        }
    }

    bool request_stop() noexcept {
        const uint64_t one = 1;
        ssize_t n;

        do {
            n = write(_stop_eventfd, &one, sizeof(one));
        } while (n < 0 && errno == EINTR);

        if (n == sizeof(one)) return true;
        return n < 0 && errno == EAGAIN;
    }

    bool register_method(const std::string &service, const std::string &method, handler work) {
        if (service.empty() || method.empty() || !work) {
            return false;
        }
        _methods[service][method] = work;
        return true;
    }

private:
    bool create_service(const std::string &ip, uint64_t port, const std::string &key) {
        std::string val = ip + ":" + std::to_string(port);
        return _registry.registry(key, val);
    }

    void create_methods() {
        // echo示例
        // _methods["service"]["test"] = [](const std::string& payload, nexus_rpc::Rpc_response& response) {
        //     response.set_status(0);
        //     response.set_payload(payload);
        // };
    }

    int Accept(Inetaddr &client_addr) {
        return accept4(
            _socket.get_fd(), 
            client_addr.get_addr(), 
            client_addr.get_len(),
            SOCK_NONBLOCK | SOCK_CLOEXEC 
        );
    }

private:
    Socket _socket;
    int _stop_eventfd;
    Epoller _epoll;
    Registry _registry{host};
    std::unique_ptr<Rpc_user> _user;
    std::unordered_map<std::string, std::unordered_map<std::string, handler>> _methods;
    std::vector<std::unique_ptr<Rpc_loop>> _rpc_loops;
    std::vector<int> _loop_eventfds;
    std::vector<std::vector<int>> _conn_fds;
    std::array<std::mutex, reactor_num> _conn_mutexs;
    std::vector<std::thread> _loop_threads;
    thread_pool _thread_pool;
};
