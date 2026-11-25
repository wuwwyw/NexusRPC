#pragma once
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <chrono>
#include "Rpc_connection.hpp"

class u_connection_pool {
public:
    using Connection = std::unique_ptr<Rpc_connection>;

    class Connection_lease {
    public:
        Connection_lease() = default;
        Connection_lease(u_connection_pool *pool, Connection conn, const std::string &addr) 
            : _pool(pool), _conn(std::move(conn)) 
        {
            _addr = addr;
        }

        ~Connection_lease() {
            reset();
        }

        Connection_lease(Connection_lease&) = delete;
        Connection_lease& operator=(Connection_lease&) = delete;

        explicit operator bool() const {
            return _conn != nullptr;
        }

        bool& get_reuse() {
            return _reusable;
        }

        Rpc_connection* operator->() {
            return _conn.get();
        }
    private:
        void reset() {
            if (!_conn || !_pool) return;

            if (!_reusable) {
                if (!_conn->reConnect(_addr)) {
                    _conn->invalidate();
                }
            }

            _pool->release(_addr, std::move(_conn));
        }
    
    private:
        u_connection_pool *_pool = nullptr;
        Connection _conn = nullptr;
        std::string _addr;
        bool _reusable = false;
    };

public:
    Connection_lease acquire(const std::string &addr, Clock::time_point deadline) {
        Connection conn = nullptr;
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - Clock::now()
        );

        if (remaining.count() <= 0) {
            return {};
        }

        {
            std::unique_lock<std::mutex> lock(_mutex);

            bool success = _cv.wait_for(lock, remaining, [this, &addr] () {
                return !_idle_connections[addr].empty();
            });
            if (!success) return {};

            conn = std::move(_idle_connections[addr].back());
            _idle_connections[addr].pop_back();
        }
        
        if ((conn->get_connected() && conn->is_valid()) || conn->reConnect(addr)) {
            return Connection_lease(this, std::move(conn), addr);
        }

        {
            std::lock_guard<std::mutex> lock(_mutex);
            conn->get_connected() = false;
            _idle_connections[addr].push_back(std::move(conn));
        }
        _cv.notify_one();
        return {};
    }

    void put(const std::string &addr, Connection conn) {
        if (!conn) return ;

        std::lock_guard<std::mutex> lock(_mutex);
        _idle_connections[addr].push_back(std::move(conn));
    }

    void sync_addressed(const std::unordered_set<std::string> &service_set) {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (auto it = _service_addr.begin(); it != _service_addr.end();) {
                if (service_set.find(*it) == service_set.end()) {
                    _idle_connections.erase(*it);
                    it = _service_addr.erase(it);
                } else {
                    ++it;
                }
            }
        }
        
        std::unique_lock<std::mutex> lock(_mutex, std::defer_lock);

        for (auto it : service_set) {
            lock.lock();
            if (_service_addr.find(it) == _service_addr.end()) {
                lock.unlock();
                auto pos = it.find(':');
                if (pos == std::string::npos) {
                    LOG_WARN("invalid addr");
                    continue;
                }

                std::string ip = it.substr(0, pos);
                uint64_t port = 0;
                try {
                    port = std::stoi(it.substr(pos + 1));
                } catch (const std::exception &e) {
                    LOG_WARN("invalid addr");
                    continue;
                };

                auto conn1 = std::make_unique<Rpc_connection>(ip, port);
                auto conn2 = std::make_unique<Rpc_connection>(ip, port);
                auto conn3 = std::make_unique<Rpc_connection>(ip, port);
                lock.lock();
                _service_addr.insert(it);
                _idle_connections[it].push_back(std::move(conn1));
                _idle_connections[it].push_back(std::move(conn2));
                _idle_connections[it].push_back(std::move(conn3));
            }
            lock.unlock();
            _cv.notify_all();
        }
    }

private:
    void release(const std::string &addr, Connection conn) {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_service_addr.find(addr) == _service_addr.end()) return ;
            conn->get_in_buffur().clear();
            conn->get_out_buffur().clear();
            _idle_connections[addr].push_back(std::move(conn));
        }
        _cv.notify_one();
    }

private:
    std::mutex _mutex;
    std::unordered_set<std::string> _service_addr;
    std::unordered_map<std::string, std::vector<Connection>> _idle_connections;
    std::condition_variable _cv;
};