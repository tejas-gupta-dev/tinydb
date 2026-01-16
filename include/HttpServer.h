#pragma once
#include <string>
#include "DBEngine.h"

namespace tinydb {

class HttpServer {
public:
    HttpServer(DBEngine& db, int port);
    void run();

private:
    DBEngine& db;
    int port;

    std::string handleRequest(const std::string& request);
    std::string getBody(const std::string& req);
    std::string getPath(const std::string& req);
};

}
