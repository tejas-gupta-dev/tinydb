#include "TableFile.h"
#include "Constants.h"
#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace tinydb {

TableFile::TableFile(const std::string& p) : path(p) { ensureExists(); }

void TableFile::ensureExists() const {
    if(!std::filesystem::exists(path)){
        std::ofstream out(path, std::ios::binary);
        out.close();
    }
}

uint32_t TableFile::pageCount() const {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    auto size = in.tellg();
    if(size < 0) return 0;
    return (uint32_t)(size / PAGE_SIZE);
}

std::vector<uint8_t> TableFile::readPageRaw(uint32_t pageId){
    std::ifstream in(path, std::ios::binary);
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    in.seekg((std::streamoff)pageId * PAGE_SIZE, std::ios::beg);
    in.read((char*)buf.data(), PAGE_SIZE);
    if(in.gcount()!=PAGE_SIZE){
        SlottedPage p; return p.toBytes();
    }
    return buf;
}

void TableFile::writePageRaw(uint32_t pageId, const std::vector<uint8_t>& bytes){
    std::fstream f(path, std::ios::in|std::ios::out|std::ios::binary);
    if(!f.is_open()) throw std::runtime_error("Cannot open file for write");
    f.seekp((std::streamoff)pageId * PAGE_SIZE, std::ios::beg);
    f.write((const char*)bytes.data(), PAGE_SIZE);
    f.flush();
}

RowId TableFile::insertRow(const std::vector<uint8_t>& row){
    uint32_t n = pageCount();
    for(uint32_t pid=0; pid<n; pid++){
        SlottedPage p;
        p.loadFromBytes(readPageRaw(pid));
        int sid = p.insert(row);
        if(sid!=-1){
            writePageRaw(pid, p.toBytes());
            return RowId{pid,(uint16_t)sid};
        }
    }
    SlottedPage np;
    int sid=np.insert(row);
    if(sid==-1) throw std::runtime_error("Row too large");
    writePageRaw(n, np.toBytes());
    return RowId{n,(uint16_t)sid};
}

std::vector<uint8_t> TableFile::readRow(const RowId& rid){
    SlottedPage p;
    p.loadFromBytes(readPageRaw(rid.pageId));
    return p.read(rid.slotId);
}

bool TableFile::updateRow(const RowId& rid, const std::vector<uint8_t>& newRow){
    SlottedPage p;
    p.loadFromBytes(readPageRaw(rid.pageId));
    bool ok=p.update(rid.slotId,newRow);
    if(ok) writePageRaw(rid.pageId,p.toBytes());
    return ok;
}

bool TableFile::deleteRow(const RowId& rid){
    SlottedPage p;
    p.loadFromBytes(readPageRaw(rid.pageId));
    bool ok=p.remove(rid.slotId);
    if(ok) writePageRaw(rid.pageId,p.toBytes());
    return ok;
}

}
