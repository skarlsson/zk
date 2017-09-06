#include <string>

#pragma once

/**
 * Class representing a cluster URI.
 *
 * Consider zk://host1.domain.com:2110,host2.domain.com:2111,host3.domain.com:2112/foo/bar
 *
 * The URI is broken down into its parts: scheme ("zk"), authority (ie. host and port),
 * path ("/foo/bar"),
 * The scheme is lower-cased.
 */
class cluster_uri {
public:
  explicit cluster_uri(std::string s);

  bool good() const { return good_; }

  std::string scheme() const { return scheme_; }

  std::string authority() const { return authority_; }

  std::string path() const { return path_; }

  std::string str() const { return scheme_ + "://" + authority_ + path_; }

private:
  bool good_;
  std::string scheme_;
  std::string authority_;
  std::string path_;
};
