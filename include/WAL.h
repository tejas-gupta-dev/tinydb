#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace tinydb {

// basic WAL: logs INSERT/UPDATE/DELETE operations (redo-only demo)
class WAL {
public:
    explicit WAL(const std::string& walPath);

    void logInsert(const std::string& table, const std::vector<uint8_t>& rowBytes);
    void logDelete(const std::string& table, uint32_t page, uint16_t slot);
    void logUpdate(const std::string& table, uint32_t page, uint16_t slot, const std::vector<uint8_t>& newBytes);

private:
    std::string path;
};

}
