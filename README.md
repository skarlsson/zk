# zk
zookeeper c++14. Uses facebook folly.

fixing the stuff that was skipped
```
#folly dependencies
sudo apt-get install g++ automake autoconf autoconf-archive libtool libboost-all-dev libevent-dev libdouble-conversion-dev libgoogle-glog-dev \
    libgflags-dev liblz4-dev liblzma-dev libsnappy-dev make zlib1g-dev binutils-dev libjemalloc-dev libssl-dev pkg-config libiberty-dev
#zookeeper deps
sudo apt-get install -y libzookeeper-mt-dev

git clone https://github.com/facebook/folly.git
git clone https://github.com/skarlsson/zk.git

cd folly

(cd folly/test && \
 rm -rf gtest && \
 wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz && \
 tar zxf release-1.8.0.tar.gz && \
 rm -f release-1.8.0.tar.gz && \
 mv googletest-release-1.8.0 gtest)


cd folly
autoreconf -ivf
./configure
make -j4
make -j4 check
sudo make install

cd ..
cd ..



cd zk


cd ..


```




### Authors:

- [Alexander Gallego](twitter.com/gallegoxx)
- [Cole Brown](twitter.com/dtcb)
- [svante karlsson]




