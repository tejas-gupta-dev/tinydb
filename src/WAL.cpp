#include "WAL.h"
#include <fstream>

namespace tinydb {

enum OpType : uint32_t { OP_INSERT=1, OP_DELETE=2, OP_UPDATE=3 };

WAL::WAL(const std::string& walPath) : path(walPath) {
    std::ofstream out(path, std::ios::app | std::ios::binary);
    out.close();
}

void WAL::logInsert(const std::string& table, const std::vector<uint8_t>& rowBytes) {
    std::ofstream out(path, std::ios::app | std::ios::binary);
    uint32_t op=OP_INSERT;
    uint32_t tlen=(uint32_t)table.size();
    uint32_t rlen=(uint32_t)rowBytes.size();
    out.write((char*)&op,4);
    out.write((char*)&tlen,4); out.write(table.data(), tlen);
    out.write((char*)&rlen,4); out.write((const char*)rowBytes.data(), rlen);
    out.flush();
}

void WAL::logDelete(const std::string& table, uint32_t page, uint16_t slot){
    std::ofstream out(path, std::ios::app | std::ios::binary);
    uint32_t op=OP_DELETE;
    uint32_t tlen=(uint32_t)table.size();
    out.write((char*)&op,4);
    out.write((char*)&tlen,4); out.write(table.data(), tlen);
    out.write((char*)&page,4);
    out.write((char*)&slot,2);
    out.flush();
}

void WAL::logUpdate(const std::string& table, uint32_t page, uint16_t slot, const std::vector<uint8_t>& newBytes){
    std::ofstream out(path, std::ios::app | std::ios::binary);
    uint32_t op=OP_UPDATE;
    uint32_t tlen=(uint32_t)table.size();
    uint32_t rlen=(uint32_t)newBytes.size();
    out.write((char*)&op,4);
    out.write((char*)&tlen,4); out.write(table.data(), tlen);
    out.write((char*)&page,4);
    out.write((char*)&slot,2);
    out.write((char*)&rlen,4);
    out.write((const char*)newBytes.data(), rlen);
    out.flush();
}

}
