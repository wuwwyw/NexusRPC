#pragma once
#include "../Tcp_IO/Inetaddr.hpp"
#include "../Tcp_IO/Socket.hpp"
#include "../common/Etcd.hpp"
#include "../common/Logger.hpp"
#include "../Tcp_IO/Epoll.hpp"
#include "../Tcp_IO/IO.hpp"
#include "Rpc_protocol.hpp"
#include <vector>

class Rpc_receiver {
public:
    Rpc_receiver(int client_fd) : _client_fd(client_fd) {
        _io.into_fd(_client_fd);
    }

    ~Rpc_receiver() {
        closed();
    }

    Rpc_receiver(const Rpc_receiver&) = delete;
    Rpc_receiver& operator=(const Rpc_receiver&) = delete;

    IO_resp send_all() {
        return _io.send_mess(_out_buffur);
    }

    IO_reqt recv_all() {
        return _io.recv_mess(_in_buffur);
    }

    void Append(const std::string &stream_reply) {
        _out_buffur.append(stream_reply);
    }

    std::string& get_in_buffur() {
        return _in_buffur;
    }

    bool out_buffur_Empty() {
        return _out_buffur.empty();
    }

    void closed() {
        if (_client_fd >= 0) {
            close(_client_fd);
            _client_fd = -1;
            _io.into_fd(-1);
        }
    }
public:
    Rpc_frame _frame;
    Rpc_frame _reply;
    std::atomic<bool> _busy{false};
    bool _rejected = false;
    bool _closed = false;
    bool _read_paused = false; 

private:
    int _client_fd;
    IO _io;
    std::string _in_buffur;
    std::string _out_buffur;
};
