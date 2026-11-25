#pragma once
#include <algorithm>
#include <cerrno>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

const int MAX_REQUEST_SIZE = 1024 * 1024 + 16;

enum class IO_resp {
    FINISHED,
    PARTIAL,
    ERROR,
    CLOSED
};

enum class IO_reqt {
    OK,
    AGAIN,
    ERROR,
    CLOSED
};

class IO {
public:
    IO() = default;
    IO(int fd) : _fd(fd) {}

    void into_fd(int fd) {
        _fd = fd;
    }

    IO_resp send_mess(std::string &out_buffur) {
        size_t sent = 0;
        while (sent < out_buffur.size()) {
            int n = send(_fd, out_buffur.data() + sent, out_buffur.size() - sent, MSG_NOSIGNAL);
            if (n < 0) {
                if (errno == EINTR) continue;
                else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                return IO_resp::ERROR;
            } else if (n == 0) {
                return IO_resp::CLOSED;
            }
            sent += n;
        }
        out_buffur.erase(0, sent);
        if (!out_buffur.empty()) return IO_resp::PARTIAL;
        return IO_resp::FINISHED;
    }

    IO_reqt recv_mess(std::string &in_buffur) {
        bool flag = false;
        while (true) {
            char buffur[4096];
            if (in_buffur.size() >= MAX_REQUEST_SIZE) {
                return IO_reqt::OK;
            }
            
            const std::size_t remaining = MAX_REQUEST_SIZE - in_buffur.size();
            int n = recv(_fd, buffur, std::min(remaining,sizeof(buffur)), 0);

            if (n < 0) {
                if (errno == EINTR) continue;
                else if (errno == EAGAIN || errno == EWOULDBLOCK) return flag ? IO_reqt::OK : IO_reqt::AGAIN;
                return IO_reqt::ERROR;
            } else if (n == 0) {
                return IO_reqt::CLOSED;
            }
            flag = true;
            in_buffur.append(buffur, n);
        }
    }

private:
    int _fd;
};
