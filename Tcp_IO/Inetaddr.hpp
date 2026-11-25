#pragma once
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

class Inetaddr {
public:
    Inetaddr() {
        _len = sizeof(_addr);
    }

    Inetaddr(const std::string &ip, uint64_t port) : _ip(ip), _port(port) {
        _addr.sin_family = AF_INET;
        _addr.sin_port = htons(_port);
        inet_pton(AF_INET, _ip.c_str(), &_addr.sin_addr);
        _len = sizeof(_addr);
    }

    void create(const std::string &ip, uint64_t port) {
        _ip = ip,
        _port = port;
        _addr.sin_family = AF_INET;
        _addr.sin_port = htons(_port);
        inet_pton(AF_INET, _ip.c_str(), &_addr.sin_addr);
        _len = sizeof(_addr);
    }

    struct sockaddr* get_addr() {
        return (struct sockaddr *) &_addr;
    }

    socklen_t* get_len() {
        return &_len;
    }

private:
    std::string _ip;
    uint64_t _port;
    sockaddr_in _addr{};
    socklen_t _len;
};