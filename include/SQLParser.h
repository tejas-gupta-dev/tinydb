#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "Schema.h"
#include "RowCodec.h"

namespace tinydb {

struct CreateTableStmt { Schema schema; };
struct InsertStmt { std::string table; std::vector<Value> values; };
struct SelectAllStmt { std::string table; };

struct WhereEq {
    std::string col;
    Value value;
};

struct SelectWhereStmt {
    std::string table;
    WhereEq where;
};

struct UpdateWhereStmt {
    std::string table;
    std::string setCol;
    Value setValue;
    WhereEq where;
};

struct DeleteWhereStmt {
    std::string table;
    WhereEq where;
};

struct JoinStmt {
    std::string leftTable;
    std::string rightTable;
    std::string leftCol;
    std::string rightCol;
};

class SQLParser {
public:
    static bool isCreateTable(const std::string& sql);
    static bool isInsert(const std::string& sql);
    static bool isSelectAll(const std::string& sql);
    static bool isSelectWhereEq(const std::string& sql);
    static bool isUpdateWhereEq(const std::string& sql);
    static bool isDeleteWhereEq(const std::string& sql);
    static bool isJoinEq(const std::string& sql);

    static CreateTableStmt parseCreateTable(const std::string& sql);
    static InsertStmt parseInsert(const std::string& sql);
    static SelectAllStmt parseSelectAll(const std::string& sql);
    static SelectWhereStmt parseSelectWhereEq(const std::string& sql);
    static UpdateWhereStmt parseUpdateWhereEq(const std::string& sql);
    static DeleteWhereStmt parseDeleteWhereEq(const std::string& sql);
    static JoinStmt parseJoinEq(const std::string& sql);
};

}
