#pragma once
#include <cstdint>
#include <vector>
#include "Constants.h"

namespace tinydb {

struct RowId { uint32_t pageId; uint16_t slotId; };

class SlottedPage {
public:
    SlottedPage();

    void loadFromBytes(const std::vector<uint8_t>& bytes);
    std::vector<uint8_t> toBytes() const;

    int insert(const std::vector<uint8_t>& row);
    std::vector<uint8_t> read(uint16_t slotId) const;
    bool update(uint16_t slotId, const std::vector<uint8_t>& newRow);
    bool remove(uint16_t slotId);

    uint16_t slotCount() const;

private:
    std::vector<uint8_t> data;

    uint16_t getSlotCount() const;
    uint16_t getFreeStart() const;
    uint16_t getFreeEnd() const;
    void setSlotCount(uint16_t v);
    void setFreeStart(uint16_t v);
    void setFreeEnd(uint16_t v);

    uint32_t slotEntryOffset(uint16_t slotId) const;
    uint16_t getSlotOffset(uint16_t slotId) const;
    uint16_t getSlotLength(uint16_t slotId) const;
    void setSlotOffset(uint16_t slotId, uint16_t off);
    void setSlotLength(uint16_t slotId, uint16_t len);
};

}
