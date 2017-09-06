#include <memory>
#include <atomic>
#include <boost/optional.hpp>
#include "zk_client.h"
#include "cluster_uri.h"

#pragma once

namespace kspp {
  class zk_leader {
  public:
    // utility functions
    static boost::optional<int32_t>
    extractIdFromEphemeralPath(const std::string &path);

    // Note that this is a very simple leader election.
    // it is prone to the herd effect. Since our scheduler list will be small
    // this is considered OK behavior.
    zk_leader(std::string zkUri,
             std::string uuid,
             std::function<void(zk_leader *)> leaderfn,
             std::function<void(int, int, std::string, zk_client *)> zkcb);

    bool isLeader() const;

    int32_t id() const;

    std::string ephemeralPath() const;

    std::string uri() const;

    std::shared_ptr<zk_client> client() const;

  private:
    void zkCbWrapper(int type, int state, std::string path, zk_client *);

    void touchZKPathSync(const std::string &path);

    void leaderElect(int type, int state, std::string path);

    cluster_uri zkUri_;
    std::string uuid_;
    std::function<void(zk_leader *)> leadercb_;
    std::function<void(int, int, std::string, zk_client *)> zkcb_;
    std::atomic<bool> isLeader_{false};
    int32_t id_{-1};
    std::shared_ptr<zk_client> zk_;
    std::string electionPath_;
  };
}
