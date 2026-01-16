#include "HttpServer.h"
#include <sstream>
#include <iostream>

#ifdef _WIN32
  #include <winsock2.h>
  #pragma comment(lib, "Ws2_32.lib")
#else
  #include <sys/socket.h>
  #include <arpa/inet.h>
  #include <unistd.h>
#endif

namespace tinydb {

HttpServer::HttpServer(DBEngine& db_, int port_) : db(db_), port(port_) {}

static std::string httpResp(int code, const std::string& body){
    std::ostringstream oss;
    oss << "HTTP/1.1 " << code << " OK\r\n";
    oss << "Content-Type: application/json\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "Connection: close\r\n\r\n";
    oss << body;
    return oss.str();
}

std::string HttpServer::getPath(const std::string& req){
    std::istringstream iss(req);
    std::string method, path;
    iss >> method >> path;
    return path;
}

std::string HttpServer::getBody(const std::string& req){
    auto pos = req.find("\r\n\r\n");
    if(pos==std::string::npos) return "";
    return req.substr(pos+4);
}

std::string HttpServer::handleRequest(const std::string& request){
    std::string path = getPath(request);

    if(path=="/health"){
        return httpResp(200, R"({"ok":true,"msg":"alive"})");
    }

    if(path=="/query"){
        auto body = getBody(request);
        std::string sql;
        auto p = body.find("sql=");
        if(p!=std::string::npos) sql = body.substr(p+4);
        else sql = body;

        auto res = db.execute(sql);
        return httpResp(200, res);
    }

    return httpResp(404, R"({"ok":false,"msg":"not found"})");
}

void HttpServer::run(){
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2,2), &wsaData);
#endif

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if(server_fd < 0){
        std::cerr << "socket failed\n";
        return;
    }

    int opt = 1;
#ifdef _WIN32
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if(bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0){
        std::cerr << "bind failed\n";
        return;
    }

    if(listen(server_fd, 10) < 0){
        std::cerr << "listen failed\n";
        return;
    }

    std::cout << "✅ TinyDB HTTP Server running on http://localhost:" << port << "\n";

    while(true){
        sockaddr_in client{};
#ifdef _WIN32
        int len = sizeof(client);
        SOCKET client_fd = accept(server_fd, (sockaddr*)&client, &len);
        if(client_fd == INVALID_SOCKET) continue;
#else
        socklen_t len = sizeof(client);
        int client_fd = accept(server_fd, (sockaddr*)&client, &len);
        if(client_fd < 0) continue;
#endif

        char buf[16384];
        int n = 0;
#ifdef _WIN32
        n = recv(client_fd, buf, sizeof(buf)-1, 0);
#else
        n = (int)read(client_fd, buf, sizeof(buf)-1);
#endif
        if(n <= 0){
#ifdef _WIN32
            closesocket(client_fd);
#else
            close(client_fd);
#endif
            continue;
        }
        buf[n]=0;
        std::string req(buf);

        auto resp = handleRequest(req);

#ifdef _WIN32
        send(client_fd, resp.c_str(), (int)resp.size(), 0);
        closesocket(client_fd);
#else
        write(client_fd, resp.c_str(), resp.size());
        close(client_fd);
#endif
    }

#ifdef _WIN32
    WSACleanup();
#endif
}

}
