cmake_minimum_required(VERSION 3.16)
project(sax)

include_directories(include)


file(GLOB_RECURSE SAX_LIB_SRC
        src/*.cc
        )
add_library(sax SHARED ${SAX_LIB_SRC})
target_link_libraries(sax tcmalloc)

#add_executable(main main.cc)
#target_link_libraries(main sax)
