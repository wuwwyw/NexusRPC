#pragma once
#include <sys/epoll.h>
#include <unistd.h>
#include <vector>

class Epoller {
public:
    Epoller() {
        _epfd = epoll_create1(EPOLL_CLOEXEC);
    }

    ~Epoller() {
        if (_epfd >= 0) {
            close(_epfd);
        }
        _epfd = -1;
    }

    bool add(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        return epoll_ctl(_epfd, EPOLL_CTL_ADD, fd, &ev) == 0;
    }

    bool del(int fd) {
        return epoll_ctl(_epfd, EPOLL_CTL_DEL, fd, nullptr) == 0;
    }

    bool mod(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        return epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &ev) == 0;
    }

    int wait(std::vector<epoll_event> &events, int time_out = -1) {
        return epoll_wait(_epfd, events.data(), events.size(), time_out);
    }

private:
    int _epfd = -1;
};