#pragma once
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <chrono>
#include "../Tcp_IO/Inetaddr.hpp"
#include "../Tcp_IO/Socket.hpp"
#include "../Tcp_IO/IO.hpp"
#include "../Tcp_IO/Epoll.hpp"
#include "../common/Logger.hpp"


using Clock = std::chrono::steady_clock;

enum class Connect_state {
    CONNECTED,
    CONNECTING,
    ERROR
};

class Rpc_connection {
public:
    Rpc_connection(
        const std::string &ip, 
        uint64_t port, 
        Clock::time_point deadline = Clock::now() + std::chrono::milliseconds(200)
    ) {
        if (!Create()) {
            LOG_ERROR("connection created failed");
            return ;
        }
        auto state = Connect(ip, port);
        if (state == Connect_state::CONNECTING) {
            _connected = wait_connect_epoll(deadline);
        } else if (state == Connect_state::CONNECTED) {
            _connected = true;
        }

        if (!_connected) {
            LOG_ERROR("connection connected failed");
            return ;
        }
    }

    ~Rpc_connection() {
        Clear();
    }

    bool is_valid() {
        const int fd = _socket.get_fd();

        if (!_connected || fd < 0) {
            return false;
        }

        if (!_in_buffur.empty() || !_out_buffur.empty()) {
            return false;
        }

        char byte;
        const ssize_t n = ::recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);

        if (n >= 0) {
            return false;
        }

        const int error = errno;
        return error == EAGAIN || error == EWOULDBLOCK;
    }

    bool reConnect(const std::string &addr) {
        Clear();
        if (!Create()) {
            return false;
        }

        auto pos = addr.find(':');
        if (pos == std::string::npos) {
            return false;
        }

        std::string ip = addr.substr(0, pos);
        uint64_t port = 0;
        try {
            port = std::stoi(addr.substr(pos + 1));
        } catch (const std::exception &e) {
            return false;
        }

        auto result = Connect(ip, port);
        Clock::time_point deadline = Clock::now() + std::chrono::milliseconds(300);

        if (result == Connect_state::CONNECTING) {
            _connected = wait_connect_epoll(deadline);
        } else if (result == Connect_state::CONNECTED) {
            _connected = true;
        }

        return _connected;
    }

    void invalidate() {
        Clear();
    }

    bool ep_add(int fd, uint32_t events) { return _wait_ep.add(fd, events); }
    bool ep_del(int fd) { return _wait_ep.del(fd); }
    bool ep_mod(int fd, uint32_t events) { return _wait_ep.mod(fd, events); }
    int ep_wait(std::vector<epoll_event> &events, int time_out = -1) { return _wait_ep.wait(events, time_out); }

    IO_resp send_all() {
        return _io.send_mess(_out_buffur);
    }
    IO_reqt recv_all() {
        return _io.recv_mess(_in_buffur);
    }

    int get_fd() {
        return _socket.get_fd();
    }

    std::string& get_in_buffur() {
        return _in_buffur;
    }
    std::string& get_out_buffur() {
        return _out_buffur;
    }

    bool& get_connected() {
        return _connected;
    }

    Rpc_connection* get() {
        return this;
    }

private:
    Connect_state Connect(const std::string &ip, uint64_t port) {
        Inetaddr server_addr(ip, port);
        int res = connect(_socket.get_fd(), server_addr.get_addr(), *server_addr.get_len());

        if (res == 0) {
            return Connect_state::CONNECTED;
        }
        if (errno == EINPROGRESS) {
            return Connect_state::CONNECTING;
        }

        return Connect_state::ERROR;
    }

    bool Create() {
        _socket.create_sockfd();
        _io.into_fd(_socket.get_fd());
        _in_buffur.clear();
        _out_buffur.clear();
        if (!_wait_ep.add(_socket.get_fd(), 0)) {
            int error = errno;
            LOG_ERROR("epoll add failed, fd = {}, error = {}", _socket.get_fd(), strerror(error));
            Clear();
            return false;
        }
        return true;
    }

    void Clear() {
        int sockfd = _socket.get_fd();
        if (sockfd >= 0) {
            _wait_ep.del(sockfd);
            _socket = Socket{};
            _io.into_fd(-1);
            _connected = false;
            _in_buffur.clear();
            _out_buffur.clear();
        }
    }

    bool wait_connect_epoll(Clock::time_point deadline) {
        std::vector <epoll_event> events(1);
        if (!_wait_ep.mod(_socket.get_fd(), EPOLLOUT)) {
            return false;
        }
        while (true) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now()
            ).count();

            if (remaining <= 0) {
                LOG_ERROR("connect timeout");
                return false;
            }

            int n = _wait_ep.wait(events, static_cast<int>(remaining));

            if (n == 0) {
                LOG_ERROR("connect timeout");
                return false;
            } else if (n < 0) {
                if (errno == EINTR) continue;

                LOG_ERROR("epoll wait failed: {}", strerror(errno));
                return false;
            }

            if (!check_connect_result()) {
                return false;
            }
            return true;
        }
    }

    bool check_connect_result() {
        int error = 0;
        socklen_t length = sizeof(error);
        if (getsockopt(_socket.get_fd(), SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
            LOG_ERROR("getsockopt failed: {}", strerror(errno));
            return false;
        }

        if (error != 0) {
            LOG_ERROR("connect failed: {}", strerror(error));
            return false;
        }

        return true;
    }

    

private:
    bool _connected = false;
    Socket _socket;
    IO _io;
    Epoller _wait_ep;
    std::string _in_buffur;
    std::string _out_buffur;
};