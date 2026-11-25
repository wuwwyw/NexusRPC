#pragma once
#include <cstdlib>
#include <utility>
#include <unistd.h>
#include "Inetaddr.hpp"
#include "../common/Logger.hpp"

enum {
    SUCCESS,
    SOCKET_FAIL,
    BIND_FAIL,
    LISTEN_FAIL
};

class Socket {
public:
    static constexpr int DEFAULT_LISTEN_BACKLOG = 1024;

    Socket() {}

    ~Socket() {
        Close();
    }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept 
        : _sockfd(std::exchange(other._sockfd, -1)) {}

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            Close();
            _sockfd = std::exchange(other._sockfd, -1);
        }
        return *this;
    }

    void create_sockfd() {
        if (_sockfd >= 0) {
            return ;
        }
        _sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (_sockfd == -1) {
            LOG_CRIT("sockfd created failed");
            exit(SOCKET_FAIL);
        }
    }

    void BindAndListen(const std::string &ip, uint64_t port, int backlog = DEFAULT_LISTEN_BACKLOG) {
        Inetaddr server_addr(ip, port);
        if (bind(_sockfd, server_addr.get_addr(), *server_addr.get_len()) == -1) {
            LOG_CRIT("sockfd bind failed");
            exit(BIND_FAIL);
        }

        if (listen(_sockfd, backlog) == -1) {
            LOG_CRIT("sockfd listen failed");
            exit(LISTEN_FAIL);
        }
    }

    int get_fd() {
        return _sockfd;
    }

private:
    void Close() {
        if (_sockfd >= 0) {
            close(_sockfd);
        }
        _sockfd = -1;
    }

private:
    int _sockfd = -1;
};
