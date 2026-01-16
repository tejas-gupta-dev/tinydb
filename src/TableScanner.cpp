#include "TableScanner.h"
#include "SlottedPage.h"

namespace tinydb {

TableScanner::TableScanner(TableFile& t) : table(t) {}

std::vector<ScanRow> TableScanner::scanAll() {
    std::vector<ScanRow> out;
    uint32_t pages = table.pageCount();

    for (uint32_t pid = 0; pid < pages; pid++) {
        SlottedPage p;
        p.loadFromBytes(table.readPageRaw(pid));
        uint16_t sc = p.slotCount();
        for (uint16_t sid = 0; sid < sc; sid++) {
            auto bytes = p.read(sid);
            if (bytes.empty()) continue;
            out.push_back(ScanRow{RowId{pid, sid}, bytes});
        }
    }
    return out;
}

}
