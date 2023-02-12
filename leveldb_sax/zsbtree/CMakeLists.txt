cmake_minimum_required(VERSION 3.16)
project(zsbtree)


include_directories(../sax/include)

file(GLOB_RECURSE ZSBTREE_LIB_SRC
        ./*.cc
        ./*.h
        )


add_library(zsbtree SHARED ${ZSBTREE_LIB_SRC})
target_link_libraries(zsbtree tcmalloc)

