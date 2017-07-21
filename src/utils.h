#pragma once

#include <string>
#include <folly/Uri.h>

std::string uuid();

std::string zookeeperHostsFromUrl(folly::Uri);