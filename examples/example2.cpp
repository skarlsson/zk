#include "zk_leader.h"
#include <iostream>
#include <deque>
#include <numeric>
#include <glog/logging.h>

using namespace std::chrono_literals;
using namespace kspp;

std::string s_zkUri("zk://127.0.0.1:2181,192.168.100.44:2181/nissegul");
const std::string s_uuid = "56912496-7904-4933-8b84-8042948df297";

void init(std::deque<std::shared_ptr<kspp::zk_leader>> &leaders, size_t count) {

  for (int i = 0; i != count; ++i) {
    leaders.push_back(std::make_shared<kspp::zk_leader>(s_zkUri,
                                                       s_uuid,
                                                       [](kspp::zk_leader *) { LOG(INFO) << "testbody leader cb"; },
                                                       [](int type, int state, std::string path, kspp::zk_client *) {
                                                         LOG(INFO) << "callback type:" << type << ", state: " << state
                                                                   << ", path:" << path;
                                                       }));
  }
}

int main(int argc, char **argv) {


  std::deque<std::shared_ptr<kspp::zk_leader>> leaders;

  init(leaders, 20);
  {
    // pop the leader (first one)
    for (;;) {
      leaders.pop_front(); // allow for zk conn to close
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      if (leaders.empty()) {
        break;
      }

      LOG(INFO) << "Leaders ID's left: "
                << std::accumulate(
                        leaders.begin(), leaders.end(), std::string(),
                        [](const std::string &a, std::shared_ptr<zk_leader> b) {
                          auto bstr = std::to_string(b->id());
                          return (a.empty() ? bstr : a + "," + bstr);
                        });


      LOG(INFO) << "Leaders left: " << leaders.size();
      bool haveLeader = false;
      int maxTries = 100;

      while (!haveLeader) {
        for (auto &ptr : leaders) {
          if (ptr->isLeader()) {
            LOG(INFO) << "Found leader: " << ptr->id();
            haveLeader = true;
            break;
          }
        }

        std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        if (maxTries-- < 0) {
          break;
        }
      }

      assert(haveLeader == true);
    }
  }


  init(leaders, 20);
  {
    int maxNumberOfAdditions = 20;
    const auto kHalfOfLuck = RAND_MAX / 2;
    for (;;) {
      if (std::rand() > kHalfOfLuck) {
        leaders.pop_front();
      } else {
        leaders.pop_back();
      }
      // allow for zk conn to close
      std::this_thread::yield();
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      if (maxNumberOfAdditions-- > 0) {
        leaders.push_back(std::make_shared<zk_leader>(
                s_zkUri,
                s_uuid,
                [](zk_leader *) { LOG(INFO) << "testbody leader cb"; },
                [](int type, int state, std::string path, zk_client *cli) {
                  LOG(INFO) << "testbody zoo cb";
                }));
        std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      if (leaders.empty()) {
        break;
      }

      LOG(INFO) << "Leaders ID's left: "
                << std::accumulate(
                        leaders.begin(), leaders.end(), std::string(),
                        [](const std::string &a, std::shared_ptr<zk_leader> b) {
                          auto bstr = std::to_string(b->id());
                          return (a.empty() ? bstr : a + "," + bstr);
                        });


      LOG(INFO) << "Leaders left: " << leaders.size();
      bool haveLeader = false;
      int maxTries = 100;

      while (!haveLeader) {
        for (auto &ptr : leaders) {
          if (ptr->isLeader()) {
            LOG(INFO) << "Found leader: " << ptr->id();
            haveLeader = true;
            break;
          }
        }

        std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        if (maxTries-- < 0) {
          break;
        }
      }

      assert(haveLeader == true);
    }
  }
  leaders.clear();

//TEST(ZookeeperLeaderEphemeralNode, id_parsing)
  {
    auto str = "asdfasdfasdf_70f7d1ad-6a4c-4ad4-b187-d33483ebd728_n_0000000002";
    auto ret = zk_leader::extractIdFromEphemeralPath(str);
    assert(ret.get() == 2);
  }

//TEST(ZookeeperLeaderEphemeralNode, id_parsing_bad_id)
  {
    auto str = "asdfasdfasdf_70f7d1ad-6a4c-4ad4-b187-d33483ebd728_asdfasdf";
    auto ret = zk_leader::extractIdFromEphemeralPath(str);
    assert(boost::none == ret);
  }

//TEST(ZookeeperLeaderEphemeralNode, id_parsing_close_but_no_cigar)
  {
    // match hast to be exactly 10 digits as specified by zk api
    auto str = "asdfasdfasdf_70f7d1ad-6a4c-4ad4-b187-d33483ebd728_n_000000002";
    auto ret = zk_leader::extractIdFromEphemeralPath(str);
    assert(boost::none == ret);
  }
}

