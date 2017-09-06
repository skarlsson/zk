
#include "cluster_uri.h"

cluster_uri::cluster_uri(std::string s)
        : good_(true) {
  std::string::size_type pos0 = s.find("://");
  if (pos0 == std::string::npos) {
    good_ = false;
    return;
  }
  scheme_ = s.substr(0, pos0);
  std::string::size_type pos1 = s.find('/', pos0 + 3);
  if (pos1 != std::string::npos) {
    authority_ = s.substr(pos0 + 3, pos1-(pos0 + 3));
    path_ = s.substr(pos1, std::string::npos);
  } else {
    authority_ = s.substr(pos0 + 3, std::string::npos);
  }
}