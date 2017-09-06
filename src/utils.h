/*#pragma once

#include <string>

class zookeeper_connection_uri {
public:
  explicit zookeeper_connection_uri(const std::string s) {
    std::string::size_type pos = s.find('/');
    if (pos != std::string::npos) {
      host_and_ports_ = s.substr(0, pos);
      path_ = s.substr(pos, std::string::npos);
    } else {
      host_and_ports_ = s;
    }
  }

  std::string hosts() const { return host_and_ports_; }

  std::string path() const { return path_; }

  std::string str() const { return host_and_ports_ + path_; }

private:
  std::string host_and_ports_;
  std::string path_;
};
*/

