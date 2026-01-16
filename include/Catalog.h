#pragma once
#include <string>
#include "Schema.h"

namespace tinydb {

class Catalog {
public:
    explicit Catalog(const std::string& dbDir);

    bool createTable(const Schema& schema);
    bool hasTable(const std::string& tableName) const;
    Schema loadSchema(const std::string& tableName) const;

    std::string tablePath(const std::string& tableName) const;
    std::string schemaPath(const std::string& tableName) const;

private:
    std::string dir;
};

}
