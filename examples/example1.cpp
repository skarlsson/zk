#include "zk_client.h"
#include <iostream>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  kspp::zk_client zk([](int type, int state, std::string path, kspp::zk_client *) {
    std::cerr << "callback type:" << type << ", state: " << state << ", path:" << path << std::endl;
  });

  {
    auto result = zk.set("/foobar", "thingo");
    assert(result.result == ZNONODE);
  }

  {
    assert(zk.exists("/foobar").ok() == false);
    auto result = zk.create("/foobar", "thingo", &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
    assert(result.ok() == true);

    assert(zk.exists("/foobar").ok() == true);
    auto nodeTuple = zk.get("/foobar");
    assert(nodeTuple.buff == "thingo");
    //assert(strcmp((char *) nodeTuple.data(), (char *) data->data()) == 0);
  }

  {
    zk.create("/foobar", "thingo", &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
    auto result = zk.set("/foobar", "asdf");
    assert(result.ok() == true);
    auto nodeTuple = zk.get("/foobar");
    assert(nodeTuple.buff == "asdf");
  }

  {
    zk.create("/foobar", "thingo", &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
    auto delResult = zk.del("/foobar");
    assert(delResult.ok() == true);
  }

  return 0;
}