add_executable(zkclient_test zkclient/zkclient_test.cc)
target_link_libraries(zkclient_test ${CSI_LIBS} gtest)


