#include "HashIndex.h"
#include <algorithm>

namespace tinydb {

void HashIndex::add(const std::string& key, const RowId& rid){
    idx[key].push_back(rid);
}

void HashIndex::remove(const std::string& key, const RowId& rid){
    auto it = idx.find(key);
    if(it==idx.end()) return;
    auto& v = it->second;
    v.erase(std::remove_if(v.begin(), v.end(), [&](const RowId& x){
        return x.pageId==rid.pageId && x.slotId==rid.slotId;
    }), v.end());
    if(v.empty()) idx.erase(it);
}

std::vector<RowId> HashIndex::find(const std::string& key) const{
    auto it = idx.find(key);
    if(it==idx.end()) return {};
    return it->second;
}

void HashIndex::clear(){ idx.clear(); }

}
