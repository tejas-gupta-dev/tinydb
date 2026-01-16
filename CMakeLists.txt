cmake_minimum_required(VERSION 3.10)
project(tinydb_full_v2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

add_executable(tinydb
    main.cpp
    src/SlottedPage.cpp
    src/TableFile.cpp
    src/Catalog.cpp
    src/RowCodec.cpp
    src/TableScanner.cpp
    src/HashIndex.cpp
    src/WAL.cpp
    src/SQLParser.cpp
    src/DBEngine.cpp
    src/HttpServer.cpp
)


if (WIN32)
    target_link_libraries(tinydb ws2_32)
endif()

