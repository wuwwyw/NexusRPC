#include "NexusRPC/Rpc_server.hpp"

int main() {
    Rpc_server server("127.0.0.1", 19090, prefix + "service/echo-demo");

    if (!server.register_method("service", "echo",
        [](const std::string& payload, nexus_rpc::Rpc_response& reply) {
            reply.set_status(0);
            reply.set_payload(payload);
        })) {
        return 1;
    }

    server.loop();
}