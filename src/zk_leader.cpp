#include <regex>
#include <set>
#include <boost/algorithm/string.hpp>
#include "zk_leader.h"
#include <glog/logging.h>

namespace kspp {
  using namespace std::placeholders;

  std::string zk_leader::uri() const { return zkUri_.str(); }

  std::shared_ptr<zk_client> zk_leader::client() const { return zk_; }


  zk_leader::zk_leader(std::string zkUri,
                       std::string uuid,
                       std::function<void(zk_leader *)> leaderfn,
                       std::function<void(int, int, std::string, zk_client *)> zkcb)
          : zkUri_(zkUri), uuid_(uuid), leadercb_(leaderfn), zkcb_(zkcb) {
    if (!zkUri_.good()) {
      LOG(FATAL) << "Bad zk uri:" << zkUri;
    }

    auto cb = std::bind(&zk_leader::zkCbWrapper, this, _1, _2, _3, _4);
    const auto baseElectionPath = zkUri_.path() + "/election";
    const auto baseElectionId = baseElectionPath + "/" + uuid_ + "_n_";
    zk_ = std::make_shared<zk_client>(cb, zkUri_.authority(), 500, 0, true /*yield until connected*/);
    LOG(INFO) << "Watching: " << baseElectionPath;
    auto zkret = zk_->exists(baseElectionPath, true);
    if (zkret.result == ZNONODE) {
      touchZKPathSync(baseElectionPath);
    } else {
      CHECK(zkret.result == ZOK) << "Failed to watch the directory ret code: " << zkret.result;
    }

    LOG(INFO) << "Creating election node: " << baseElectionId;
    zkret = zk_->create(baseElectionId, std::string(), &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE | ZOO_EPHEMERAL);
    CHECK(zkret.result == ZOK) << "Couldn't create election path: " << baseElectionId << ", ret: " << zkret.result;
    LOG(INFO) << "Ephemeral node: " << zkret.buff;
    electionPath_ = std::move(zkret.buff);
    auto optId = extractIdFromEphemeralPath(electionPath_);
    CHECK(optId) << "Could not parse id from ephemeral path";
    id_ = optId.get();
    LOG(INFO) << "Leader election id: " << id_;
    leaderElect(0, 0, "");
  }

  void zk_leader::touchZKPathSync(const std::string &nestedPath) {
    std::vector<std::string> paths;
    boost::split(paths, nestedPath, boost::is_any_of("/"));
    CHECK(!paths.empty()) << "Empty nested path: " << nestedPath;
    std::string path = "";

    for (auto p : paths) {
      if (p.empty() || p == "/") {
        continue;
      }

      path += "/" + p;
      auto zkret = zk_->exists(path);

      if (zkret.result == ZNONODE) {
        LOG(INFO) << "Creating directory: " << path;
        zkret = zk_->create(path, std::string(), &ZOO_OPEN_ACL_UNSAFE, 0);
        CHECK(zkret.result == ZOK) << "failed to create path, code: " << zkret.result;
      } else {
        CHECK(zkret.result == ZOK) << "failed to read path, code: " << zkret.result;
      }
    }
  }

  void zk_leader::leaderElect(int type, int state, std::string path) {

    const std::string baseElectionPath = zkUri_.path() + "/election";
    auto zkret = zk_->children(baseElectionPath, true);
    auto retcode = zkret.result;

    if (retcode == -1 || retcode == ZCLOSING || retcode == ZSESSIONEXPIRED) {
      LOG(ERROR) << "Zookeeper not ready|closing|expired socket [MYID: " << id_ << "] ";
      return;
    }

    if (!(retcode == ZOK || retcode == ZNONODE)) {
      LOG(ERROR) << "[MYID " << id_ << "] " << "No children: " << baseElectionPath << ", retcode: " << zkret.result;
      return;
    }

    std::set<int32_t> runningLeaders;

    for (auto &s : zkret.strings) {
      LOG(INFO) << "Running for election: [MYID: " << id_ << "] " << s
                << ", base path: " << path;
      runningLeaders.insert(extractIdFromEphemeralPath(s).get());
    }

    if (!runningLeaders.empty()) {
      if (id_ >= 0) {
        if (runningLeaders.find(id_) == runningLeaders.end()) {
          CHECK(!isLeader_) << "Cannot be leader and then not leader. Doesn't work. exit";
          isLeader_ = false;
          LOG(ERROR) << "Could not find my id [MYID: " << id_ << "]. out of sync w/ zookeeper";
        } else if (id_ <= *runningLeaders.begin()) {
          LOG(INFO) << "LEADER! - ONE ID TO RULE THEM ALL: " << id_;
          if (!isLeader_) {
            // ONLY CALL ONCE
            leadercb_(this);
          }
          isLeader_ = true;
        }
      }
    }
  }

  void zk_leader::zkCbWrapper(int type, int state, std::string path, zk_client *cli) {
    LOG(INFO) << "Notification received[MYID: " << id_
              << "]. type: " << zk_client::printZookeeperEventType(type)
              << ", state: " << zk_client::printZookeeperState(state)
              << ", path: " << path;

    if (id_ > 0) {
      leaderElect(type, state, path);
    }
    zkcb_(type, state, path, cli);
  }

  bool zk_leader::isLeader() const { return isLeader_; }

  int32_t zk_leader::id() const { return id_; }

  std::string zk_leader::ephemeralPath() const { return electionPath_; }

  boost::optional<int32_t>
  zk_leader::extractIdFromEphemeralPath(const std::string &path) {
    // zookeeper ephemeral ids are always guaranteed to end in 10
    // monotonically increasing digits by the C api - see zookeeper.h
    static const std::regex zkid(".*(\\d{10})$");
    std::smatch what;

    if (std::regex_search(path, what, zkid)) {
      return std::stoi(what[1].str());
    }

    return boost::none;
  }
}
