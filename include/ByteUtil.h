#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <algorithm>
#include <stdexcept>

namespace tinydb {

inline void write_u16(uint8_t* buf, uint16_t v){ std::memcpy(buf, &v, 2); }
inline void write_u32(uint8_t* buf, uint32_t v){ std::memcpy(buf, &v, 4); }

inline uint16_t read_u16(const uint8_t* buf){ uint16_t v; std::memcpy(&v, buf, 2); return v; }
inline uint32_t read_u32(const uint8_t* buf){ uint32_t v; std::memcpy(&v, buf, 4); return v; }

inline void append_u32(std::vector<uint8_t>& b, uint32_t v){
    uint8_t tmp[4]; std::memcpy(tmp,&v,4);
    b.insert(b.end(), tmp, tmp+4);
}

inline void append_i32(std::vector<uint8_t>& b, int32_t v){
    uint8_t tmp[4]; std::memcpy(tmp,&v,4);
    b.insert(b.end(), tmp, tmp+4);
}

inline uint32_t pop_u32(const std::vector<uint8_t>& b, size_t& pos){
    if(pos+4>b.size()) throw std::runtime_error("pop_u32 overflow");
    uint32_t v; std::memcpy(&v, &b[pos], 4); pos+=4; return v;
}
inline int32_t pop_i32(const std::vector<uint8_t>& b, size_t& pos){
    if(pos+4>b.size()) throw std::runtime_error("pop_i32 overflow");
    int32_t v; std::memcpy(&v, &b[pos], 4); pos+=4; return v;
}

inline std::string trim(const std::string& s){
    size_t a = s.find_first_not_of(" \t\n\r");
    size_t b = s.find_last_not_of(" \t\n\r");
    if(a==std::string::npos) return "";
    return s.substr(a,b-a+1);
}

inline std::string upper(std::string s){
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return (char)std::toupper(c); });
    return s;
}

}
