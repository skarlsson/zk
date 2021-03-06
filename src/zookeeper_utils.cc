#include "zookeeper_utils.hpp"
#include <set>
#include <glog/logging.h>

namespace kspp {
  void failFastOnZooKeeperGet(int rc) {
    static std::set<int> failstatus{
            ZNOAUTH, ZBADARGUMENTS, ZINVALIDSTATE, ZMARSHALLINGERROR};
    if (failstatus.find(rc) != failstatus.end()) {
      LOG(FATAL) << "Bad status for zookeeper get: " << rc;
    }
  }
}
