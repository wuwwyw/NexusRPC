#pragma once
#include "../common/Config.hpp"
#include <cstddef>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <arpa/inet.h>
#include <charconv>
#include <system_error>

struct node {
    std::string key, val;
};

struct services_node {
    std::vector<node> nodes;
    std::unordered_map <std::string, size_t> pos;
};

class service_table {
public:
    void put(const std::string &key, const std::string &val) {
        if (!valid_rpc_address(val)) {
            del(key);
            return;
        }

        std::lock_guard <std::mutex> lock(_mutex);

        if (key.compare(0, prefix.size(), prefix) != 0) {
            return;
        }

        size_t pre = prefix.size();
        size_t suf = key.find('/', pre);

        if (suf == std::string::npos ||
            suf == pre ||
            suf + 1 == key.size()) {
            return;
        }

        std::string cate_key = key.substr(pre, suf - pre);
        std::string addr_key = key.substr(suf + 1);

        auto &bucket = _services[cate_key];
        auto idx = bucket.pos.find(addr_key);
        if (idx != bucket.pos.end()) {
            bucket.nodes[idx->second] = {addr_key, val};
        } else {
            bucket.nodes.push_back({addr_key, val});
            bucket.pos[addr_key] = bucket.nodes.size() - 1;
        }
    }

    void del(const std::string &key) {
        std::lock_guard <std::mutex> lock(_mutex);

        if (key.compare(0, prefix.size(), prefix) != 0) {
            return;
        }

        size_t pre = prefix.size();
        size_t suf = key.find('/', pre);

        if (suf == std::string::npos ||
            suf == pre ||
            suf + 1 == key.size()) {
            return;
        }

        std::string cate_key = key.substr(pre, suf - pre);
        std::string addr_key = key.substr(suf + 1);

        auto &bucket = _services[cate_key]; 
        auto idx = bucket.pos.find(addr_key);
        if (idx == bucket.pos.end()) return ;

        const size_t id = idx->second;
        const size_t last = bucket.nodes.size() - 1;

        if (id != last) {
            bucket.nodes[id] = std::move(bucket.nodes[last]);
            bucket.pos[bucket.nodes[id].key] = id;
        }

        bucket.nodes.pop_back();
        bucket.pos.erase(idx);
    }

    void replace(const std::vector<node> &snapshot) {
        service_table fresh;
        for (auto &it : snapshot) {
            fresh.put(it.key, it.val);
        }
        std::lock_guard<std::mutex> lock(_mutex);
        _services.swap(fresh._services);
    }

    std::unordered_set <std::string> address() {
        std::unordered_set<std::string> service_tables;
        std::lock_guard<std::mutex> lock(_mutex);
        
        for (auto &it : _services) {
            for (auto &instance : it.second.nodes) {
                service_tables.insert(instance.val);
            }
        }
        return service_tables;
    }

    bool choose_addr(const std::string &key_category, std::string &addr) {
        std::lock_guard <std::mutex> lock(_mutex);
        auto it = _services.find(key_category);
        if (it == _services.end()) return false;

        auto& nodes = it->second.nodes;
        if (nodes.empty()) return false;

        std::uniform_int_distribution<size_t> dist(0, nodes.size() - 1);
        addr = _services[key_category].nodes[dist(_rng)].val;
        return true;
    }

private:
    bool valid_rpc_address(const std::string &addr) {
        const auto separator = addr.find(':');

        if (separator == std::string::npos ||
            separator == 0 ||
            separator + 1 == addr.size() ||
            addr.find('\0') != std::string::npos) {
            return false;
        }

        const std::string ip = addr.substr(0, separator);
        in_addr ipv4{};
        if (inet_pton(AF_INET, ip.c_str(), &ipv4) != 1) {
            return false;
        }

        const char *first = addr.data() + separator + 1;
        const char *last = addr.data() + addr.size();

        unsigned int port = 0;
        const auto result = std::from_chars(first, last, port);

        return result.ec == std::errc{} &&
            result.ptr == last &&
            port >= 1 && port <= 65535;
    }
    

private:
    std::mt19937 _rng{std::random_device{}()};
    std::mutex _mutex;
    std::unordered_map <std::string, services_node> _services;
};

