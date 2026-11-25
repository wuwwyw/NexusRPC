#pragma once
#include "Rpc_service_table.hpp"
#include "U_connection_pool.hpp"
#include "../common/Etcd.hpp"
#include "../common/Logger.hpp"
#include "../Tcp_IO/Inetaddr.hpp"
#include "../Tcp_IO/Socket.hpp"
#include "../Tcp_IO/IO.hpp"
#include "../Tcp_IO/Epoll.hpp"
#include "Rpc_protocol.hpp"
#include "proto/rpc.pb.h"
#include <charconv>


class Rpc_user {
public:
    Rpc_user()
        : _discover(
            host,
            prefix,
            [this] (const std::string &key, const std::string &val) {
                _table.put(key, val);
                _u_pool.sync_addressed(_table.address());
            },
            [this] (const std::string &key, const auto&) {
                _table.del(key);
                _u_pool.sync_addressed(_table.address());
            },
            [this] (const Discover::Snapshot &snapshot) -> void {
                std::vector<node> nodes;
                nodes.reserve(snapshot.size());
                for (const auto &[key,val] : snapshot) {
                    nodes.push_back({key, val});
                }
                _table.replace(nodes);
                _u_pool.sync_addressed(_table.address());
            }
        ) {}
    
    bool Call(
        const std::string &service, 
        const std::string &method, 
        const std::string &payload, 
        nexus_rpc::Rpc_response &res,
        int time_out_ms = 100000,
        Rpc_error *err = nullptr) 
    {
        Rpc_error local_error;
        if (err == nullptr) {
            err = &local_error;
        }
        *err = Rpc_error::NONE;
        res.Clear();

        if (time_out_ms <= 0) {
            *err = Rpc_error::INVALID_ARGUMENT;
            return false;
        }
        auto deadline = Clock::now() + chrono::milliseconds(time_out_ms);

        std::string addr;
        if (!choose_addr(service, addr)) {
            LOG_ERROR("no available server for service: {}", service);
            *err = Rpc_error::NO_SERVER;
            return false;
        }

        auto conn = _u_pool.acquire(addr, deadline);

        if (!conn) {
            *err = Rpc_error::CONNECT_FAILED;
            return false;
        }

        int sockfd = conn->get_fd();

        if (!conn->ep_mod(sockfd, EPOLLOUT)) {
            int error = errno;
            LOG_ERROR("epoll mod failed, fd = {}, error = {}", sockfd, strerror(error));
            *err = Rpc_error::INTERNAL_ERROR;
            return false;
        }

        uint64_t request_id = 0;
        if (!make_request(service, method, payload, request_id, *err, conn)) {
            LOG_ERROR("make request failed");

            return false;
        }

        if (!send_to(deadline, *err, conn)) {
            return false;
        }

        if (!conn->ep_mod(sockfd, EPOLLIN)) {
            int error = errno;
            LOG_ERROR("epoll mod failed, fd = {}, error = {}", sockfd, strerror(error));
            *err = Rpc_error::INTERNAL_ERROR;
            return false;
        }

        if (!recv_in(res, request_id, deadline, *err, conn)) {
            res.Clear();
            return false;
        }

        if (sockfd != -1 && !conn->ep_mod(sockfd, 0)) {
            int error = errno;
            LOG_ERROR("epoll mod failed, fd = {}, error = {}", sockfd, strerror(error));
            return true;
        }
        conn.get_reuse() = true;
        return true;
    }

private:
    bool make_request(
        const std::string &service, 
        const std::string &method, 
        const std::string &payload, 
        uint64_t &request_id,
        Rpc_error &err,
        u_connection_pool::Connection_lease &conn
    ) {
        std::string &out_buffur = conn->get_out_buffur();
        nexus_rpc::Rpc_request request;
        request.set_service(service);
        request.set_method(method);
        request.set_payload(payload);
        
        Rpc_frame frame;
        if (!request.SerializeToString(&frame._body)) {
            err = Rpc_error::PROTOCOL_ERROR;
            return false;
        }
        if (frame._body.size() > RPC_MAX_BODY_SIZE) {
            err = Rpc_error::REQUEST_TOO_LARGE;
            return false;
        }

        frame._header.type = Rpc_type::REQUEST;
        frame._header.body_size = static_cast<uint32_t>(frame._body.size());
        frame._header.request_id = ++_next_request_id;
        
        out_buffur.append(frame.serialize());
        request_id = _next_request_id;

        return true;
    }

    bool choose_addr (const std::string &key_cate, std::string &addr) {
        return _table.choose_addr(key_cate, addr);
    }

    bool send_to(Clock::time_point deadline, Rpc_error &err, u_connection_pool::Connection_lease &conn) {
        vector <epoll_event> events(1);
        IO_resp flag = IO_resp::PARTIAL;

        while (flag != IO_resp::FINISHED) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now()
            ).count();

            if (remaining <= 0) {
                LOG_ERROR("send timeout");
                err = Rpc_error::TIMEOUT;
                return false;
            }

            int n = conn->ep_wait(events, remaining);
            if (n == 0) {
                LOG_ERROR("send timeout");
                err = Rpc_error::TIMEOUT;
                return false;
            } else if (n < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR("wait send failed");
                err = Rpc_error::INTERNAL_ERROR;
                return false;
            }

            flag = conn->send_all();
            if (flag == IO_resp::ERROR) {
                LOG_ERROR("request send failed");
                err = Rpc_error::IO_ERROR;
                return false;
            } else if (flag == IO_resp::CLOSED) {
                LOG_INFO("server closed");
                err = Rpc_error::IO_ERROR;
                return false;
            }
        }

        return true;
    }

    bool recv_in(
        nexus_rpc::Rpc_response &res, 
        uint64_t request_id, 
        Clock::time_point deadline, 
        Rpc_error &err,
        u_connection_pool::Connection_lease &conn
    ) {
        Rpc_frame frame;
        vector <epoll_event> events(1);
        std::string &in_buffur = conn->get_in_buffur();
        
        while (true) {
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now()
            ).count();

            if (remaining <= 0) {
                LOG_ERROR("recv timeout");
                err = Rpc_error::TIMEOUT;
                return false;
            }

            int n = conn->ep_wait(events, remaining);
            if (n == 0) {
                LOG_ERROR("recv timeout");
                err = Rpc_error::TIMEOUT;
                return false;
            } else if (n < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR("wait recv failed");
                err = Rpc_error::INTERNAL_ERROR;
                return false;
            }

            auto flag = conn->recv_all();
            if (flag == IO_reqt::ERROR) {
                LOG_ERROR("response recevice failed");
                err = Rpc_error::IO_ERROR;
                return false;
            } else if (flag == IO_reqt::CLOSED) {
                LOG_INFO("server closed");
                Parse_result result = Parse_result::INCOMPLETE;
                if (!oper(frame, request_id, result, conn)) {
                    err = Rpc_error::PROTOCOL_ERROR;
                    return false;
                }

                if (result != Parse_result::COMPLETE) {
                    err = Rpc_error::IO_ERROR;
                    return false;
                }

                break;
                
            } else if (flag == IO_reqt::OK) {
                Parse_result result = Parse_result::INCOMPLETE;
                if (!oper(frame, request_id, result, conn)) {
                    err = Rpc_error::PROTOCOL_ERROR;
                    return false;
                }

                if (result == Parse_result::COMPLETE) {
                    break;
                }
            }
        }

        if (!res.ParseFromString(frame._body)) {
            LOG_ERROR("invalid protobuf response");
            in_buffur.clear();
            err = Rpc_error::PROTOCOL_ERROR;
            return false;
        }

        return true;
    }

    bool oper(Rpc_frame &frame, uint64_t &request_id, Parse_result &result, u_connection_pool::Connection_lease &conn) {
        std::string &in_buffur = conn->get_in_buffur();
        if (!frame._flag_header) {
            result = frame.check_post(in_buffur);
            if (result == Parse_result::INVALID) {
                LOG_ERROR("post invalid");
                in_buffur.clear();
                return false;
            } else if (result == Parse_result::COMPLETE) {
                if (frame._header.request_id != request_id || frame._header.type != Rpc_type::RESPONSE) {
                    LOG_ERROR("invalid request_id or type");
                    in_buffur.clear();
                    return false;
                }

                frame._flag_header = true;
            } else {
                return true;
            }
        }

        size_t frame_size = frame._header.body_size + RPC_HEADER_SIZE;
        result = frame.check_data(frame_size, in_buffur);
        return true;
    }

private:
    uint64_t _next_request_id = 0;
    u_connection_pool _u_pool;
    service_table _table;
    Discover _discover;
};
