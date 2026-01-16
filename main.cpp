#include <iostream>
#include "DBEngine.h"
#include <cstdlib>
#include "HttpServer.h"

using namespace tinydb;

int main(){
    DBEngine db("data");

    std::cout << "✅ TinyDB Full v2 (UPDATE/DELETE/JOIN added)\n";
    std::cout << "1) Start HTTP server on :8080\n";
    std::cout << "2) CLI mode\n";
    std::cout << "Choose (1/2): ";

    int mode=2;
    std::cin >> mode;
    std::cin.ignore();

    if(mode==1){
        int port = 8080;
        const char* p = std::getenv("PORT");
        if (p) port = std::atoi(p);
        HttpServer server(db, port);

        server.run();
        return 0;
    }

    while(true){
        std::cout << "\nTinyDB> ";
        std::string sql;
        std::getline(std::cin, sql);
        if(sql=="exit"||sql=="quit") break;
        if(sql.empty()) continue;

        std::cout << db.execute(sql) << "\n";
    }
    return 0;
}
