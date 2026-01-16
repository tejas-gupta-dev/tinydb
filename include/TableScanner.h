#pragma once
#include <cstdint>
#include <vector>
#include "TableFile.h"

namespace tinydb {

struct ScanRow {
    RowId rid;
    std::vector<uint8_t> bytes;
};

class TableScanner {
public:
    explicit TableScanner(TableFile& table);

    std::vector<ScanRow> scanAll();

private:
    TableFile& table;
};

}
