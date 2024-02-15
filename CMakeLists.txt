cmake_minimum_required(VERSION 3.25)
project(LSM_TREE)

set(CMAKE_CXX_STANDARD 17)

add_executable(LSM_TREE  kvstore.cc kvstore.h MurmurHash3.h  test.h utils.h SkipList.h SkipList.cpp SSList.h SSTable.cpp Buffer.h Buffer.cpp SSList.cpp  correctness.cc)
