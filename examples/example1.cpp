#include "ZKClient.hpp"
#include <iostream>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  bolt::ZKClient zk([](int type, int state, std::string path, bolt::ZKClient *) {
    std::cerr << "callback type:" << type << ", state: " << state << ", path:" << path << std::endl;
  });

  {
    auto data = folly::IOBuf::copyBuffer("thingo", 6);
    auto result = zk.setSync("/foobar", std::move(data));
    assert(result.result == ZNONODE);
  }

  {
    assert(zk.existsSync("/foobar").ok() == false);
    auto data = folly::IOBuf::copyBuffer("thingo", 7);
    auto result = zk.createSync("/foobar", std::move(data), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
    assert(result.ok() == true);

    assert(zk.existsSync("/foobar").ok() == true);
    auto nodeTuple = zk.getSync("/foobar");
    assert(strcmp((char *) nodeTuple.data(), (char *) data->data()) == 0);
  }

  {
    auto data = folly::IOBuf::copyBuffer("thingo", 7);
    zk.createSync("/foobar", std::move(data), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
    auto data2 = folly::IOBuf::copyBuffer("asdf", 5);
    auto result = zk.setSync("/foobar", std::move(data2));
    assert(result.ok() == true);
    auto nodeTuple = zk.getSync("/foobar");
    assert(strcmp((char *) nodeTuple.data(), (char *) data2->data()) == 0);
  }

  {
    auto data = folly::IOBuf::copyBuffer("thingo", 7);
    zk.createSync("/foobar", std::move(data), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL);
    auto delResult = zk.delSync("/foobar");
    assert(delResult.ok() == true);
  }

  return 0;
}