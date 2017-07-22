cmake_minimum_required(VERSION 2.8.11)

set(CMAKE_CXX_STANDARD 14)

find_package(Boost COMPONENTS regex log_setup log thread program_options filesystem system REQUIRED)
set(BOOST_LIBS
        ${Boost_LOG_LIBRARY}
        ${Boost_LOG_SETUP_LIBRARY}
        ${Boost_PROGRAM_OPTIONS_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_THREAD_LIBRARY}
        ${Boost_SYSTEM_LIBRARY}
        ${Boost_REGEX_LIBRARY}
        pthread
        rt
        c
        )

SET(CSI_INCLUDE_PATH ${Boost_INCLUDE_DIR})

SET(CSI_INCLUDE_PATH
        ${CSI_INCLUDE_PATH}
        ${CMAKE_SOURCE_DIR}/../zk/src
        ${CMAKE_SOURCE_DIR}/../folly
        )
file(GLOB zk_lib_files
        ${CMAKE_CURRENT_SOURCE_DIR}/../zk/src/*.*
        )
add_library(csi-zk STATIC ${zk_lib_files})
SET(LIBZK_LIBS csi-zk zookeeper_mt folly glog)

SET(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
add_definitions(-D_FILE_OFFSET_BITS=64 -D_REENTRANT -DEXTERNAL_LOCKS -DMULTITHREAD)
add_definitions(-fPIC)
add_definitions(-DBOOST_LOG_DYN_LINK)
add_definitions(-Wno-deprecated)
SET(CSI_LIBS ${LIBZK_LIBS} ${BOOST_LIBS} crypto ssl)

include_directories(${CSI_INCLUDE_PATH} ${CMAKE_SOURCE_DIR})
link_directories(${CSI_LIBRARY_PATH})

#TARGETS
add_subdirectory(examples)
#add_subdirectory(test)

