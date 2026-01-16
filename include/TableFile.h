#pragma once
#include <string>
#include <cstdint>
#include <vector>
#include "SlottedPage.h"

namespace tinydb {

class TableFile {
public:
    explicit TableFile(const std::string& path);

    RowId insertRow(const std::vector<uint8_t>& row);
    std::vector<uint8_t> readRow(const RowId& rid);
    bool updateRow(const RowId& rid, const std::vector<uint8_t>& newRow);
    bool deleteRow(const RowId& rid);

    uint32_t pageCount() const;
    std::vector<uint8_t> readPageRaw(uint32_t pageId);

private:
    std::string path;
    void ensureExists() const;
    void writePageRaw(uint32_t pageId, const std::vector<uint8_t>& bytes);
};

}
