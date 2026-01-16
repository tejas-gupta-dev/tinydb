#pragma once
#include <unordered_map>
#include <string>
#include <vector>
#include "SlottedPage.h"

namespace tinydb {

class HashIndex {
public:
    void add(const std::string& key, const RowId& rid);
    void remove(const std::string& key, const RowId& rid);
    std::vector<RowId> find(const std::string& key) const;
    void clear();

private:
    std::unordered_map<std::string, std::vector<RowId>> idx;
};

}
