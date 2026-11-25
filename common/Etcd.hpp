#pragma once
#include <cstdint>
#include <unordered_set>
#include <functional>
#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Response.hpp>
#include <etcd/SyncClient.hpp>
#include <etcd/Value.hpp>
#include <etcd/Watcher.hpp>
#include "Config.hpp"
#include "Logger.hpp"

class Registry {
public:
    Registry(const std::string &host) {
        _client = std::make_shared<etcd::Client>(host);
        _kp_alive_fail = false;
        _stop = false;
        _registered = false;
        _keep_alive = std::make_shared<etcd::KeepAlive>(
            *_client,
            [this] (std::exception_ptr) {
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _kp_alive_fail = true;
                }
                
                _cv_kp.notify_all();
            },
            3
        );
        _lease_id = _keep_alive->Lease();

        _rot = std::move(std::thread([this] () {
            while (true) {
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    _cv_kp.wait(
                        lock,
                        [this] { return (_kp_alive_fail && _registered) || _stop; }
                    );
                    if (_stop) return ;
                    _kp_alive_fail = false;
                }
                
                while (!recover()) {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (_cv_kp.wait_for(
                        lock, 
                        std::chrono::milliseconds(500),
                        [this] { return _stop; }
                    )) {
                        return ;
                    }
                }
            }
        }));
    }

    ~Registry() {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _stop = true;
        }
        _cv_kp.notify_all();
        if (_rot.joinable()) _rot.join();
        _keep_alive->Cancel();
    }


    bool registry(const std::string &key, const std::string &val) {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_registered) {
                LOG_ERROR("this Registry already has a registered service");
                return false;
            }
        }

        try {
            auto res = _client->put(key, val, _lease_id).get();
            if (!res.is_ok()) {
                LOG_ERROR("register service failed: {}", res.error_message());
                return false;
            }

            {
                std::lock_guard<std::mutex> lock(_mutex);
                _key = key;
                _val = val;
                _registered = true;
            }
            _cv_kp.notify_all();

            return true;
        } catch (const exception &e) {
            LOG_ERROR("register service failed: {}", e.what());
            return false;
        }
    }

private:
    bool recover() {
        try {
            _keep_alive = std::make_shared<etcd::KeepAlive>(
                *_client,
                [this] (std::exception_ptr) {
                    {
                        std::lock_guard<std::mutex> lock(_mutex);
                        _kp_alive_fail = true;
                    }
                    
                    _cv_kp.notify_all();
                },
                3
            );
            _lease_id = _keep_alive->Lease();

            auto res = _client->put(_key, _val, _lease_id).get();
            if (!res.is_ok()) {
                LOG_ERROR("recover service failed: {}", res.error_message());
                return false;
            }
            return true;
        } catch (const exception &e) {
            LOG_ERROR("recover service failed: {}", e.what());
            return false;
        }
    }

private:
    std::string _key;
    std::string _val;
    bool _registered;
    bool _kp_alive_fail;
    bool _stop;
    std::mutex _mutex;
    std::condition_variable _cv_kp;
    std::thread _rot;
    std::shared_ptr <etcd::Client> _client;
    std::shared_ptr <etcd::KeepAlive> _keep_alive;
    int64_t _lease_id;
};


class Discover {
public:
    using Notify_callback = std::function<void(const std::string&, const std::string&)>;
    using Snapshot = std::vector<std::pair<std::string,std::string>>;
    using Snapshot_callback = std::function<void(const Snapshot&)>;
    using Service_set = std::unordered_set<std::string>;
    

    Discover(
        const std::string &host, 
        const std::string &basedir, 
        Notify_callback put_cb, 
        Notify_callback del_cb,
        Snapshot_callback snap_cb = {}
    ) {
        _client = std::make_shared <etcd::Client>(host);
        _put_cb = std::move(put_cb);
        _del_cb = std::move(del_cb);
        _snap_cb = std::move(snap_cb);

        const int64_t next_revision = load_snapshot(basedir);

        _watcher = std::make_shared <etcd::Watcher> (
            *_client.get(), 
            basedir,
            next_revision,
            std::bind(&Discover::callback, this, std::placeholders::_1),
            true 
        );

        _rct = std::move(std::thread([this] (const std::string &dir) {
            connectORstop(dir);
        }, basedir));
    }

    ~Discover() {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _stop = true;
        }
        _retry_cv.notify_all();
        if (_watcher) _watcher->Cancel();
        _rct.join();
    }

private:
    int64_t load_snapshot(const std::string& basedir) {
        auto res = _client->ls(basedir).get();
        if (!res.is_ok()) {
            LOG_ERROR("cannot get service data: {}", res.error_message());
            throw std::runtime_error("cannot load services: " + res.error_message());
        }

        Snapshot snapshot;
        snapshot.reserve(res.keys().size());
        size_t sz = res.keys().size();
        for (size_t i = 0; i < sz; i++) {
            snapshot.push_back({res.key(i), res.value(i).as_string()});
        };
        if (_snap_cb) {
            _snap_cb(snapshot);
        } else if (_put_cb) {
            for (size_t i = 0; i < sz; i++) {
                _put_cb(res.key(i), res.value(i).as_string());
            }
        }

        return res.index() + 1;
    }

    void connectORstop(const std::string &basedir) {
        while (true) {
            {
                std::lock_guard<std::mutex> lock(_mutex);
                if (_stop) {
                    if (_watcher) _watcher->Cancel();
                    return ;
                }
            }
            
            _watcher->Wait();

            while (true) {
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (_retry_cv.wait_for(
                        lock, 
                        std::chrono::milliseconds(500),
                        [this] { return _stop; }
                    )) {
                        return ;
                    }
                }
                
                try {
                    const int64_t next_revision = load_snapshot(basedir);
                    auto next = std::make_shared <etcd::Watcher> (
                        *_client.get(), 
                        basedir,
                        next_revision,
                        std::bind(&Discover::callback, this, std::placeholders::_1),
                        true 
                    );

                    {
                        std::lock_guard<std::mutex> lock(_mutex);
                        if (_stop) return ;
                        _watcher.swap(next);
                    }
                    
                    break;
                } catch (const std::exception &e) {
                    LOG_ERROR("etcd reconnected failed: {}", e.what());
                }
            }
        }
    }

    void callback(const etcd::Response &res) {
        if (!res.is_ok()) {
            LOG_ERROR("recv a bad notify: {}", res.error_message());
            return ;
        }

        for (const auto &ev : res.events()) {
            if (ev.event_type() == etcd::Event::EventType::PUT) {
                if (_put_cb) _put_cb(ev.kv().key(), ev.kv().as_string());
                LOG_INFO("add a service: {} - {}", ev.kv().key(), ev.kv().as_string());
            } else if (ev.event_type() == etcd::Event::EventType::DELETE_) {
                if (ev.has_prev_kv()) {
                    const auto &old = ev.prev_kv();

                    if (_del_cb) {
                        _del_cb(old.key(), old.as_string());
                    }

                    LOG_INFO("del a service: {} - {}", old.key(), old.as_string());
                } else {
                    if (_del_cb) {
                        _del_cb(ev.kv().key(), "");
                    }

                    LOG_INFO("del a service: {}, previous value unavailable", ev.kv().key());
                }
            }
        }
    }

private:
    bool _stop = false;
    std::mutex _mutex;
    std::condition_variable _retry_cv;
    std::thread _rct;
    Notify_callback _put_cb;
    Notify_callback _del_cb;
    Snapshot_callback _snap_cb;
    std::shared_ptr <etcd::Client> _client;
    std::shared_ptr <etcd::Watcher> _watcher;
};
