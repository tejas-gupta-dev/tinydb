#include "Catalog.h"
#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace tinydb {

Catalog::Catalog(const std::string& dbDir) : dir(dbDir) {
    if (!std::filesystem::exists(dir)) {
        std::filesystem::create_directories(dir);
    }
}

std::string Catalog::tablePath(const std::string& tableName) const {
    return dir + "/" + tableName + ".tbl";
}

std::string Catalog::schemaPath(const std::string& tableName) const {
    return dir + "/" + tableName + ".schema";
}

bool Catalog::hasTable(const std::string& tableName) const {
    return std::filesystem::exists(schemaPath(tableName)) &&
           std::filesystem::exists(tablePath(tableName));
}

bool Catalog::createTable(const Schema& schema) {
    if (schema.tableName.empty()) return false;
    if (schema.columns.empty()) return false;
    if (hasTable(schema.tableName)) return false;

    std::ofstream out(schemaPath(schema.tableName), std::ios::binary);
    if (!out.is_open()) return false;

    out << schema.tableName << "\n";
    out << schema.columns.size() << "\n";
    for (auto& c : schema.columns) {
        out << c.name << " " << (int)c.type << "\n";
    }
    out.close();

    std::ofstream tfile(tablePath(schema.tableName), std::ios::binary);
    tfile.close();
    return true;
}

Schema Catalog::loadSchema(const std::string& tableName) const {
    std::ifstream in(schemaPath(tableName), std::ios::binary);
    if (!in.is_open()) throw std::runtime_error("Schema not found: " + tableName);

    Schema s;
    std::getline(in, s.tableName);

    size_t n = 0;
    in >> n;
    in.ignore();

    for (size_t i = 0; i < n; i++) {
        Column col;
        int t;
        in >> col.name >> t;
        in.ignore();
        col.type = (ColType)t;
        s.columns.push_back(col);
    }
    return s;
}

}
