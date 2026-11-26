#include "NexusRPC/Rpc_uesr.hpp"
#include <iostream>

int main() {
    Rpc_user client;
    nexus_rpc::Rpc_response reply;

    if (!client.Call("service", "echo", "hello", reply, 3000)) {
        return 1;
    }

    if (reply.status() != 0 || reply.payload() != "hello") {
        return 1;
    }

    std::cout << reply.payload() << '\n';
}