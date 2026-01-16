#pragma once
#include <string>
#include <unordered_map>
#include "Catalog.h"
#include "HashIndex.h"
#include "WAL.h"

namespace tinydb {

class DBEngine {
public:
    explicit DBEngine(const std::string& dbDir);
    std::string execute(const std::string& sql);

private:
    Catalog catalog;
    WAL wal;

    std::unordered_map<std::string, std::unordered_map<std::string, HashIndex>> indexes;

    void buildIndexIfMissing(const std::string& table, const Schema& schema);
    void rebuildIndexes(const std::string& table, const Schema& schema);
    int colIndex(const Schema& schema, const std::string& col) const;

    std::string jsonEscape(const std::string& s) const;
};

}
