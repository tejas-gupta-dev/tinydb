#include "SlottedPage.h"
#include "ByteUtil.h"
#include <stdexcept>
#include <cstring>

namespace tinydb {

static constexpr uint16_t HEADER_SIZE = 6;
static constexpr uint16_t SLOT_ENTRY_SIZE = 4;

SlottedPage::SlottedPage() {
    data.assign(PAGE_SIZE, 0);
    setSlotCount(0);
    setFreeStart(HEADER_SIZE);
    setFreeEnd(PAGE_SIZE);
}

void SlottedPage::loadFromBytes(const std::vector<uint8_t>& bytes) {
    if(bytes.size()!=PAGE_SIZE) throw std::runtime_error("Invalid page size");
    data = bytes;
}

std::vector<uint8_t> SlottedPage::toBytes() const { return data; }

uint16_t SlottedPage::getSlotCount() const { return read_u16(&data[0]); }
uint16_t SlottedPage::getFreeStart() const { return read_u16(&data[2]); }
uint16_t SlottedPage::getFreeEnd() const { return read_u16(&data[4]); }

void SlottedPage::setSlotCount(uint16_t v){ write_u16(&data[0], v); }
void SlottedPage::setFreeStart(uint16_t v){ write_u16(&data[2], v); }
void SlottedPage::setFreeEnd(uint16_t v){ write_u16(&data[4], v); }

uint32_t SlottedPage::slotEntryOffset(uint16_t slotId) const {
    return HEADER_SIZE + slotId * SLOT_ENTRY_SIZE;
}
uint16_t SlottedPage::getSlotOffset(uint16_t slotId) const {
    return read_u16(&data[slotEntryOffset(slotId)]);
}
uint16_t SlottedPage::getSlotLength(uint16_t slotId) const {
    return read_u16(&data[slotEntryOffset(slotId)+2]);
}
void SlottedPage::setSlotOffset(uint16_t slotId, uint16_t off){
    write_u16(&data[slotEntryOffset(slotId)], off);
}
void SlottedPage::setSlotLength(uint16_t slotId, uint16_t len){
    write_u16(&data[slotEntryOffset(slotId)+2], len);
}

uint16_t SlottedPage::slotCount() const { return getSlotCount(); }

int SlottedPage::insert(const std::vector<uint8_t>& row) {
    uint16_t rowLen = (uint16_t)row.size();
    if(rowLen==0) return -1;

    uint16_t sc = getSlotCount();
    uint16_t fs = getFreeStart();
    uint16_t fe = getFreeEnd();

    uint16_t need = SLOT_ENTRY_SIZE + rowLen;
    if(fe < fs || (fe-fs) < need) return -1;

    uint16_t rowOff = (uint16_t)(fe - rowLen);
    std::memcpy(&data[rowOff], row.data(), rowLen);

    setSlotOffset(sc, rowOff);
    setSlotLength(sc, rowLen);

    setSlotCount(sc+1);
    setFreeStart(fs + SLOT_ENTRY_SIZE);
    setFreeEnd(rowOff);

    return sc;
}

std::vector<uint8_t> SlottedPage::read(uint16_t slotId) const {
    uint16_t sc = getSlotCount();
    if(slotId>=sc) return {};
    uint16_t len = getSlotLength(slotId);
    if(len==0) return {};
    uint16_t off = getSlotOffset(slotId);
    if(off+len > PAGE_SIZE) return {};
    std::vector<uint8_t> out(len);
    std::memcpy(out.data(), &data[off], len);
    return out;
}

bool SlottedPage::update(uint16_t slotId, const std::vector<uint8_t>& newRow) {
    uint16_t sc = getSlotCount();
    if(slotId>=sc) return false;
    uint16_t oldLen = getSlotLength(slotId);
    if(oldLen==0) return false;

    uint16_t oldOff = getSlotOffset(slotId);
    uint16_t newLen = (uint16_t)newRow.size();

    if(newLen <= oldLen){
        std::memcpy(&data[oldOff], newRow.data(), newLen);
        setSlotLength(slotId, newLen);
        return true;
    }
    return false;
}

bool SlottedPage::remove(uint16_t slotId) {
    uint16_t sc = getSlotCount();
    if(slotId>=sc) return false;
    uint16_t len = getSlotLength(slotId);
    if(len==0) return false;
    setSlotLength(slotId, 0);
    setSlotOffset(slotId, 0);
    return true;
}

}
