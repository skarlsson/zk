#include <future>
#include <tuple>
#include <functional>
#include <type_traits>
#include <utility>
#include <thread>
#include <vector>
#include <atomic>
#include <limits>
#include <mutex>
#include <boost/optional.hpp>
#include <zookeeper/zookeeper.h>

#pragma once

namespace kspp {
  class zk_client;

  struct ZKResult {
    ZKResult(int rc,
             boost::optional<Stat> stat = boost::none,
             std::string val = std::string())
            : result(rc), status(stat), buff(std::move(val)) {}

    //const uint8_t *data() const { return buff->data(); }

    bool ok() {
      return result == ZOK; // might need something else
    }

    int result = -1;
    // struct Stat {
    //     int64_t czxid;
    //     int64_t mzxid;
    //     int64_t ctime;
    //     int64_t mtime;
    //     int32_t version;
    //     int32_t cversion;
    //     int32_t aversion;
    //     int64_t ephemeralOwner;
    //     int32_t dataLength;
    //     int32_t numChildren;
    //     int64_t pzxid;
    // };
    // int serialize_Stat(struct oarchive *out
    //                   , const char *tag, struct Stat *v);
    // int deserialize_Stat(struct iarchive *in
    //                     , const char *tag, struct Stat*v);
    // void deallocate_Stat(struct Stat*); std::future
    boost::optional<Stat> status;
    std::string buff;
    // represents the strings api of zookeeper
    // when the appropriate call is made - i.e.: zoo_wget_children2
    // cannot be a std::set! must keep zoo api fidelity.
    // struct String_vector {
    //     int32_t count;
    //     char * *data;
    // };
    // int serialize_String_vector(struct oarchive *out
    //                         , const char *tag, struct String_vector *v);
    // int deserialize_String_vector(struct iarchive *in
    //                           , const char *tag, struct String_vector *v);
    // int allocate_String_vector(struct String_vector *v, int32_t len);
    // int deallocate_String_vector(struct String_vector *v);
    std::vector<std::string> strings;
  };

  typedef std::function<void(int, int, const std::string, zk_client *)> ZKWatchCb;

  class zk_client {
  public:
    static std::string printZookeeperEventType(int type);

    static std::string printZookeeperState(int state);

    static bool retryable(int rc);

    static void rawInitHandle(zk_client *);


    enum { NO_TIMEOUT = std::numeric_limits<int>::max() };

    template<class F>
    zk_client(F &&watch,
             std::string hosts = "127.0.0.1:2181",
             int timeout = 30, // ms std::future
             int flags = 0,
             bool block = true)
            : watch_(watch), ready(false), hosts_(hosts), timeout_(timeout), flags_(flags) {
      init(block);
    }

    ~zk_client();

    std::future<ZKResult> children_async(std::string path, bool watch = false);

    ZKResult children(std::string path, bool watch = false);

    std::future<ZKResult> get_async(std::string path, bool watch = false);

    ZKResult get(std::string path, bool watch = false);

    std::future<ZKResult>
    set_async(std::string path, std::string val, int version = -1);

    ZKResult set(std::string path, std::string val, int version = -1);

    std::future<ZKResult> exists_async(std::string path, bool watch = false);

    ZKResult exists(std::string path, bool watch = false);

    std::future<ZKResult> create_async(std::string path,
                                       std::string val,
                                       ACL_vector *acl,
                                       int flags);

    ZKResult create(std::string path,
                    std::string val,
                    ACL_vector *acl,
                    int flags);

    std::future<ZKResult> del_async(std::string path, int version = -1);

    ZKResult del(std::string path, int version = -1);

    const clientid_t *getClientId();

    // State constants
    int getState() { return zoo_state(zoo_); }

    int64_t getSessionId() { return zoo_client_id(zoo_)->client_id; }


    void decrementSessionTries() { maxSessionConnTries_--; }

    int getSessionsTriesLeft() const { return maxSessionConnTries_; }

    int timeout() const { return timeout_; }

    int flags() const { return flags_; }

    std::string hosts() { return hosts_; }

    // The following should be considered private API
    // needed for the callback. XXX (agallego,bigs):
    // not part of public api - internal - use pimpl idom
    // and refactor
    ZKWatchCb watch_;
    std::atomic<bool> ready;
    zhandle_t *zoo_{nullptr};
    std::mutex rawInitMutex_;

    void destroy();

    void init(bool block);

  private:
    const std::string hosts_;
    int timeout_;
    int flags_;
    int maxSessionConnTries_ = {600};
  };
}
