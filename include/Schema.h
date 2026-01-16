#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace tinydb {

enum class ColType {
    INT32 = 1,
    TEXT  = 2
};

struct Column {
    std::string name;
    ColType type;
};

struct Schema {
    std::string tableName;
    std::vector<Column> columns;
};

}
