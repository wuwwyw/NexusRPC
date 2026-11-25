#pragma once
#include <cstdint>
#include <string>

inline constexpr size_t RPC_HEADER_SIZE = 16;
inline constexpr uint32_t RPC_MAX_BODY_SIZE = 1024 * 1024;

enum class Rpc_error {
    NONE,
    INVALID_ARGUMENT,
    NO_SERVER,
    INVALID_ADDRESS,
    CONNECT_FAILED,
    TIMEOUT,
    IO_ERROR,
    PROTOCOL_ERROR,
    REQUEST_TOO_LARGE,
    INTERNAL_ERROR
};

enum class Parse_result {
    INCOMPLETE,
    INVALID,
    COMPLETE
};

enum class Rpc_type : uint8_t {
    REQUEST  = 1,
    RESPONSE = 2
};

struct Rpc_header {
    uint16_t magic = 0x4E58;                //2
    uint8_t version = 1;                    //1
    Rpc_type type = Rpc_type::REQUEST;      //1
    uint32_t body_size = 0;                 //4
    uint64_t request_id = 0;                //8
};

struct Rpc_frame {
    Rpc_header _header;
    std::string _body;
    bool _flag_header = false;

    void append_be(uint64_t value, size_t count, std::string &result) {
        for (size_t i = count; i > 0; --i) {
            result.push_back(static_cast<char>(
                (value >> ((i - 1) * 8)) & 0xFF
            ));
        }
    }
    
    uint64_t read_be(const unsigned char *p, size_t count) {
        uint64_t val = 0;
        for (size_t i = 0; i < count; i++) {
            val = (val << 8) | p[i];
        }
        return val;
    }

    std::string serialize() {
        std::string result;
        result.reserve(RPC_HEADER_SIZE + _body.size());

        append_be(_header.magic, 2, result);
        append_be(_header.version, 1, result);
        append_be(static_cast<uint8_t>(_header.type), 1, result);
        append_be(_header.body_size, 4, result);
        append_be(_header.request_id, 8, result);

        result.append(_body);
        return result;
    }

    Parse_result check_all(std::string &in_buffur) {
        if (in_buffur.size() < RPC_HEADER_SIZE) return Parse_result::INCOMPLETE;
        const auto *data = reinterpret_cast<const unsigned char *>(in_buffur.data());

        Rpc_header header;
        header.magic = static_cast<uint16_t>(read_be(data, 2));
        header.version = data[2];
        header.type = static_cast<Rpc_type>(data[3]);
        header.body_size = static_cast<uint32_t>(read_be(data + 4, 4));
        header.request_id = static_cast<uint64_t>(read_be(data + 8, 8));

        if (
            header.magic != 0x4E58 ||
            header.version != 1 ||
            (header.type != Rpc_type::REQUEST &&
            header.type != Rpc_type::RESPONSE) ||
            header.body_size > RPC_MAX_BODY_SIZE
        ) {
            return Parse_result::INVALID;
        }

        size_t size = header.body_size + RPC_HEADER_SIZE;
        if (in_buffur.size() < size) return Parse_result::INCOMPLETE;

        return Parse_result::COMPLETE;
    }

    Parse_result check_post(std::string &in_buffur) {
        if (in_buffur.size() < RPC_HEADER_SIZE) return Parse_result::INCOMPLETE;

        const auto *data = reinterpret_cast<const unsigned char *>(in_buffur.data());

        Rpc_header header;
        header.magic = static_cast<uint16_t>(read_be(data, 2));
        header.version = data[2];
        header.type = static_cast<Rpc_type>(data[3]);
        header.body_size = static_cast<uint32_t>(read_be(data + 4, 4));
        header.request_id = static_cast<uint64_t>(read_be(data + 8, 8));

        if (
            header.magic != 0x4E58 ||
            header.version != 1 ||
            (header.type != Rpc_type::REQUEST &&
            header.type != Rpc_type::RESPONSE) ||
            header.body_size > RPC_MAX_BODY_SIZE
        ) {
            return Parse_result::INVALID;
        }

        _header = header;
        return Parse_result::COMPLETE;
    }

    Parse_result check_data(size_t size, std::string &in_buffur) {
        if (in_buffur.size() < size) return Parse_result::INCOMPLETE;

        std::string request = in_buffur.substr(RPC_HEADER_SIZE, _header.body_size);
        in_buffur.erase(0, size);
        _body = request;
        return Parse_result::COMPLETE;
    }
};