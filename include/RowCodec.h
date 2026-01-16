#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include "Schema.h"

namespace tinydb {

using Value = std::variant<int32_t, std::string>;

class RowCodec {
public:
    static std::vector<uint8_t> encode(const Schema& schema, const std::vector<Value>& values);
    static std::vector<Value> decode(const Schema& schema, const std::vector<uint8_t>& bytes);
    static std::string toString(const Schema& schema, const std::vector<Value>& values);

    static std::string valueToKey(const Value& v);
};

}
