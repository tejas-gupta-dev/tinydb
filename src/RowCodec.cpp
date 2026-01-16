#include "RowCodec.h"
#include "ByteUtil.h"
#include <stdexcept>
#include <sstream>

namespace tinydb {

std::vector<uint8_t> RowCodec::encode(const Schema& schema, const std::vector<Value>& values) {
    if (values.size() != schema.columns.size()) {
        throw std::runtime_error("values count mismatch schema");
    }

    std::vector<uint8_t> out;
    append_u32(out, 1); // row version

    for (size_t i = 0; i < schema.columns.size(); i++) {
        auto& col = schema.columns[i];

        if (col.type == ColType::INT32) {
            if (!std::holds_alternative<int32_t>(values[i]))
                throw std::runtime_error("type mismatch INT32 at " + col.name);
            append_i32(out, std::get<int32_t>(values[i]));
        } else if (col.type == ColType::TEXT) {
            if (!std::holds_alternative<std::string>(values[i]))
                throw std::runtime_error("type mismatch TEXT at " + col.name);
            auto s = std::get<std::string>(values[i]);
            append_u32(out, (uint32_t)s.size());
            out.insert(out.end(), s.begin(), s.end());
        } else {
            throw std::runtime_error("Unknown column type");
        }
    }
    return out;
}

std::vector<Value> RowCodec::decode(const Schema& schema, const std::vector<uint8_t>& bytes) {
    std::vector<Value> out;
    size_t pos = 0;
    (void)pop_u32(bytes, pos); // version

    for (auto& col : schema.columns) {
        if (col.type == ColType::INT32) {
            out.push_back(pop_i32(bytes, pos));
        } else if (col.type == ColType::TEXT) {
            uint32_t len = pop_u32(bytes, pos);
            if (pos + len > bytes.size()) throw std::runtime_error("decode TEXT overflow");
            std::string s(bytes.begin() + pos, bytes.begin() + pos + len);
            pos += len;
            out.push_back(s);
        }
    }
    return out;
}

std::string RowCodec::toString(const Schema& schema, const std::vector<Value>& values) {
    std::ostringstream oss;
    oss << "{ ";
    for (size_t i = 0; i < schema.columns.size(); i++) {
        oss << schema.columns[i].name << "=";
        if (std::holds_alternative<int32_t>(values[i])) oss << std::get<int32_t>(values[i]);
        else oss << "\"" << std::get<std::string>(values[i]) << "\"";
        if (i + 1 < schema.columns.size()) oss << ", ";
    }
    oss << " }";
    return oss.str();
}

std::string RowCodec::valueToKey(const Value& v){
    if(std::holds_alternative<int32_t>(v)) return std::to_string(std::get<int32_t>(v));
    return std::get<std::string>(v);
}

}
