#pragma once
#include "Rpc_receiver.hpp"
#include "Rpc_uesr.hpp"
#include "Rpc_protocol.hpp"
#include "../Tcp_IO/IO.hpp"
#include "../common/thread_pool.hpp"
#include "proto/rpc.pb.h"
#include <cerrno>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unordered_map>

class Rpc_server {
public:
    using iter = std::unordered_map<int, std::shared_ptr<Rpc_receiver>>::iterator;
    using handler = std::function<void(const std::string&, nexus_rpc::Rpc_response&)>;

    Rpc_server(const std::string &ip, uint64_t port, const std::string &key) {
        _socket.create_sockfd();
        _socket.BindAndListen(ip, port);

        if ((_del_eventfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) < 0) {
            LOG_CRIT("del_eventfd create fail");
            exit(1);
        }

        struct itimerspec ts{};
        ts.it_interval.tv_sec  = 1;
        ts.it_interval.tv_nsec = 0;
        ts.it_value.tv_sec     = 1;
        ts.it_value.tv_nsec    = 0;
        if (timerfd_settime(_del_eventfd, 0, &ts, nullptr) != 0) {
            LOG_CRIT("timerfd_settime fail: {}", strerror(errno));
            exit(1);
        }

        if ((_quit_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
            LOG_CRIT("quit_eventfd create fail");
            exit(1);
        }

        if ((_stop_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
            LOG_CRIT("stop_eventfd create fail");
            exit(1);
        }

        if (!_epoll.add(_socket.get_fd(), EPOLLIN)) {
            LOG_CRIT("server sockfd join epoll failed, fd = {}", _socket.get_fd());
            exit(1);
        }

        if (!_epoll.add(_del_eventfd, EPOLLIN)) {
            LOG_CRIT("del_eventfd join epoll failed, fd = {}", _del_eventfd);
            exit(1);
        }

        if (!_epoll.add(_quit_eventfd, EPOLLIN)) {
            LOG_CRIT("quit_eventfd join epoll failed, fd = {}", _quit_eventfd);
            exit(1);
        }

        if (!_epoll.add(_stop_eventfd, EPOLLIN)) {
            LOG_CRIT("stop_eventfd join epoll failed, fd = {}", _quit_eventfd);
            exit(1);
        }

        if (!create_service(ip, port, key)) {
            LOG_CRIT("create service failed");
            exit(1);
        }
        
        create_methods();
    }

    ~Rpc_server() {
        close(_del_eventfd);
        {
            std::lock_guard<std::mutex> lock(_mutex_quit);
            close(_quit_eventfd);
        }
        close(_stop_eventfd);
    }

    void run() {
        while (true) {
            std::vector <epoll_event> events(1024);
            int n = _epoll.wait(events);

            if (n < 0) {
                if (errno == EINTR) continue;

                LOG_ERROR("epoll wait failed: {}", strerror(errno));
                return;
            }

            for (int i = 0; i < n; ++i) {
                if (events[i].data.fd == _stop_eventfd && (events[i].events & EPOLLIN)) {
                    std::lock_guard<std::mutex> lock(_mutex_quit);
                    _stop = true;
                    return;
                }
            }

            for (int i = 0; i < n; i++) {
                int fd = events[i].data.fd;
                if (fd == _socket.get_fd()) {
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

                        if (!_epoll.add(client_fd, EPOLLIN)) {
                            int error = errno;
                            close(client_fd);
                            LOG_ERROR("client join epoll failed: {}", strerror(error));
                            continue;
                        }
                        
                        _receivers.try_emplace(client_fd, std::make_shared<Rpc_receiver>(client_fd));
                    }
                } else if (fd == _del_eventfd) {
                    uint64_t val;
                    read(fd, &val, sizeof(val));

                    std::lock_guard<std::mutex> lock1(_mutex_del);
                    for (auto it = _delete.begin(); it != _delete.end();) {
                        int client_fd = it->first;
                        auto client = it->second;
                        std::lock_guard<std::mutex> lock2(_mutex_out);
                        if (!client->_busy.load() && (client->_rejected || client->out_buffur_Empty())) {
                            ++it;
                            Erase(client_fd, client);
                        } else {
                            it++;
                        }
                    }
                } else if (fd == _quit_eventfd) {
                    uint64_t val;
                    read(fd, &val, sizeof(val));

                    std::lock_guard<std::mutex> lock1(_mutex_quit);
                    for (auto it = _quit.begin(); it != _quit.end();) {
                        int client_fd = it->first;
                        auto client = it->second;
                        client->_busy.store(false);
                        it = _quit.erase(it);

                        if (client->_rejected) {
                            continue;
                        }

                        Parse_result full_flag;
                        {
                            std::lock_guard<std::mutex> lock2(_mutex_in);
                            auto &buffer = client->get_in_buffur();
                            client->_read_paused = buffer.size() >= MAX_REQUEST_SIZE;
                            full_flag = client->_frame.check_all(client->get_in_buffur());
                        }
                        
                        if (full_flag == Parse_result::COMPLETE) {
                            client->_busy.store(true);
                            auto result = try_dispatch(client_fd, client);
                            if (!result) {
                                client->_rejected = true;
                                client->_busy.store(false);
                            }
                        } else if (full_flag == Parse_result::INVALID) {
                            check(client_fd, client);
                            continue;
                        }

                        judgeANDset_mod(client, client_fd);
                    }
                } else if (fd != _stop_eventfd) {
                    auto it = _receivers.find(fd);
                    if (it == _receivers.end()) continue;

                    int client_fd = fd;
                    auto &client = it->second;
                    uint32_t revents = events[i].events;
                    if (revents & EPOLLERR) {
                        int err = 0;
                        socklen_t len = sizeof(err);

                        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0) {
                            LOG_ERROR("socket error, fd = {}, error = {}, message = {}", fd, err, strerror(err));
                        }

                        check(client_fd, client);
                        continue;
                    }

                    if ((revents & EPOLLHUP) && !(revents & EPOLLIN)) {
                        check(client_fd, client);
                        continue;
                    }

                    if (revents & EPOLLIN) {
                        IO_reqt flag;
                        ssize_t sz = 0;
                        if (client->_busy.load()) {
                            std::lock_guard<std::mutex> lock(_mutex_in);
                            flag = client->recv_all();
                            sz = client->get_in_buffur().size();
                        } else {
                            flag = client->recv_all();
                            sz = client->get_in_buffur().size();
                        }

                        client->_read_paused = (sz >= MAX_REQUEST_SIZE);

                        if (flag == IO_reqt::OK) {
                            if (!client->_busy.load()) {
                                client->_busy.store(true);
                                auto result = try_dispatch(client_fd, client);
                                if (!result) {
                                    client->_rejected = true;
                                    client->_busy.store(false);
                                }
                            }
                            judgeANDset_mod(client, client_fd);
                        } else if (flag == IO_reqt::ERROR) {
                            LOG_ERROR("recv error, fd = {}", client_fd);
                            Delete(client_fd, client);
                            judgeANDset_mod(client, client_fd);
                        } else if (flag == IO_reqt::CLOSED) {
                            client->_closed = true;

                            Delete(client_fd, client);
                            if (!client->_busy.load()) {
                                client->_busy.store(true);
                                auto result = try_dispatch(client_fd, client);
                                if (!result) {
                                    client->_rejected = true;
                                    client->_busy.store(false);
                                }
                            }
                            judgeANDset_mod(client, client_fd);
                        }
                    }

                    if (_receivers.find(client_fd) == _receivers.end()) continue;

                    if (revents & EPOLLOUT) {
                        IO_resp flag;
                        if (client->_busy.load()) {
                            std::lock_guard<std::mutex> lock(_mutex_out);
                            flag =client->send_all();
                        } else {
                            flag =client->send_all();
                        }
                        
                        if (flag == IO_resp::FINISHED) {
                            {
                                std::lock_guard<std::mutex> lock(_mutex_del);
                                if (_delete.find(client_fd) != _delete.end()) {
                                    if (!client->_busy.load()) {
                                        Erase(client_fd, client);
                                        continue;
                                    }
                                }
                            }

                            judgeANDset_mod(client, client_fd);
                        } else if (flag == IO_resp::CLOSED) {
                            client->_rejected = true;
                            check(client_fd, client);
                        } else if (flag == IO_resp::ERROR) {
                            LOG_ERROR("send error, fd = {}", fd);
                            client->_rejected = true;
                            check(client_fd, client);
                        }
                    }
                }
            }
        }
    }

    bool register_method(const std::string &service, const std::string &method, handler work) {
        if (service.empty() || method.empty() || !work) {
            return false;
        }
        _methods[service][method] = work;
        return true;
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

private:
    bool create_service(const std::string &ip, uint64_t port, const std::string &key) {
        std::string val = ip + ":" + std::to_string(port);
        return _registry.registry(key, val);
    }

    void create_methods() {
        // 还没写具体业务，暂时先这样
        _methods["service"]["test"] = [](const std::string& payload, nexus_rpc::Rpc_response& response) {
            response.set_status(0);
            response.set_payload(payload);
        };
        // 之后改成调用Register
        // Register(const std::string &service, const std::string &method, handler &work)
    }

    void Delete(int client_fd, const std::shared_ptr<Rpc_receiver> &client) {
        std::lock_guard<std::mutex> lock(_mutex_del);
        _delete[client_fd] = client;
    }

    int Accept(Inetaddr &client_addr) {
        return accept4(
            _socket.get_fd(), 
            client_addr.get_addr(), 
            client_addr.get_len(),
            SOCK_NONBLOCK | SOCK_CLOEXEC 
        );
    }

    void judgeANDset_mod(const shared_ptr<Rpc_receiver> &client, int client_fd) {
        uint32_t events = 0;

        if (!client->_rejected) {
            if (!client->_closed && !client->_read_paused) {
                events |= EPOLLIN;
            }

            {
                std::lock_guard<std::mutex> _lock(_mutex_out);
                
                if (!client->out_buffur_Empty()) {
                    events |= EPOLLOUT;
                }
            }
        }

        if (!_epoll.mod(client_fd, events)) {
            LOG_ERROR("epoll change event failed, fd = {}", client_fd);
            client->_rejected = true;
            check(client_fd, client);
        }
    }

    void Erase(int client_fd, const std::shared_ptr<Rpc_receiver> &client) {  
        if (_delete.find(client_fd) != _delete.end()) {
            _delete.erase(client_fd);
        }
        
        _epoll.del(client_fd);
        _receivers.erase(client_fd);
    }

    void check(int client_fd, const std::shared_ptr<Rpc_receiver> &client) {
        if (client->_busy.load()) {
            Delete(client_fd, client);
        } else {
            std::lock_guard<std::mutex> lock(_mutex_del);
            Erase(client_fd, client);
        }
    }

    void quit(int client_fd, const std::shared_ptr<Rpc_receiver> &client) {
        std::lock_guard<std::mutex> lock(_mutex_quit);
        if (_stop) return;

        _quit[client_fd] = client;
        uint64_t val = 1;
        write(_quit_eventfd, &val, sizeof(val));
    }

    bool try_dispatch(int client_fd, const std::shared_ptr<Rpc_receiver> &client) {
        return  _thread_pool.submit(std::move([this, client, client_fd] () {
                    try_parse(client, client_fd);
                    quit(client_fd, client);
                }));
    }

    bool try_parse(const std::shared_ptr<Rpc_receiver> &client, int client_fd) {
        Rpc_frame &frame = client->_frame;
        Rpc_frame &reply = client->_reply;

        while (true) {
            if (!client->_frame._flag_header) {
                Parse_result post_flag;
                {
                    std::lock_guard<std::mutex> lock(_mutex_in);
                    post_flag = frame.check_post(client->get_in_buffur());
                }
                if (post_flag == Parse_result::INCOMPLETE) {
                    break;
                } else if (post_flag == Parse_result::INVALID || frame._header.type != Rpc_type::REQUEST) {
                    LOG_ERROR("post invlid");
                    Delete(client_fd, client);

                    return false;
                }

                client->_frame._flag_header = true;
            }

            size_t frame_size = frame._header.body_size + RPC_HEADER_SIZE;
            Parse_result data_flag;
            {
                std::lock_guard<std::mutex> lock(_mutex_in);
                data_flag = frame.check_data(frame_size, client->get_in_buffur());
            }
            if (data_flag == Parse_result::INCOMPLETE) break;

            client->_frame._flag_header = false;

            nexus_rpc::Rpc_request request;
            if (!request.ParseFromString(frame._body)) {
                LOG_ERROR("invalid protobuf request, fd = {}", client_fd);
                Delete(client_fd, client);

                return false;
            }

            nexus_rpc::Rpc_response response;
            process(response, request);

            reply._header.type = Rpc_type::RESPONSE;
            reply._header.request_id = frame._header.request_id;
            
            if (!response.SerializeToString(&reply._body)) {
                LOG_ERROR("response serialize failed");
                Delete(client_fd, client);

                return false;
            }

            if (reply._body.size() > RPC_MAX_BODY_SIZE) {
                LOG_ERROR("reply body too large");
                Delete(client_fd, client);

                return false;
            }

            reply._header.body_size = static_cast<uint32_t>(reply._body.size());

            {
                std::lock_guard<std::mutex> lock(_mutex_out);
                client->Append(reply.serialize());
            }
        }

        return true;
    }

    void process(nexus_rpc::Rpc_response &response, const nexus_rpc::Rpc_request &request) {
        auto service_it = _methods.find(request.service());
        if (service_it == _methods.end()) {
            response.set_status(1);
            response.set_error_message("unknown service");
            return ;
        }

        auto method_it = service_it->second.find(request.method());
        if (method_it == service_it->second.end()) {
            response.set_status(1);
            response.set_error_message("unknown method");
            return ;
        }

        try {
            method_it->second(request.payload(), response);
        } catch (const std::exception &e) {
            LOG_ERROR("RPC handler failed: {}", e.what());
            response.Clear();
            response.set_status(2);
            response.set_error_message("internal server error");
        } catch (...) {
            LOG_ERROR("RPC handler failed: unknown exception");
            response.Clear();
            response.set_status(2);
            response.set_error_message("internal server error");
        }
    }

private:
    Socket _socket;
    Epoller _epoll;
    int _del_eventfd;
    int _quit_eventfd;
    int _stop_eventfd;
    bool _stop = false;
    std::mutex _mutex_del;
    std::mutex _mutex_quit;
    std::mutex _mutex_in;
    std::mutex _mutex_out;
    Registry _registry{host};
    std::unordered_map <int, std::shared_ptr<Rpc_receiver>> _receivers;
    std::unordered_map <int, std::shared_ptr<Rpc_receiver>> _delete;
    std::unordered_map <int, std::shared_ptr<Rpc_receiver>> _quit;
    std::unique_ptr<Rpc_user> _user;
    std::unordered_map<std::string, std::unordered_map<std::string, handler>> _methods;
    thread_pool _thread_pool;
};
