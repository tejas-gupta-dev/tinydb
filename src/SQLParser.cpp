#include "SQLParser.h"
#include "ByteUtil.h"
#include <sstream>
#include <stdexcept>

namespace tinydb {

bool SQLParser::isCreateTable(const std::string& sql){ return upper(trim(sql)).rfind("CREATE TABLE",0)==0; }
bool SQLParser::isInsert(const std::string& sql){ return upper(trim(sql)).rfind("INSERT INTO",0)==0; }
bool SQLParser::isSelectAll(const std::string& sql){ 
    auto u = upper(trim(sql));
    return u.rfind("SELECT * FROM",0)==0 && u.find("WHERE")==std::string::npos && u.find("JOIN")==std::string::npos;
}
bool SQLParser::isSelectWhereEq(const std::string& sql){
    auto u = upper(trim(sql));
    return u.rfind("SELECT * FROM",0)==0 && u.find("WHERE")!=std::string::npos;
}
bool SQLParser::isUpdateWhereEq(const std::string& sql){
    auto u = upper(trim(sql));
    return u.rfind("UPDATE",0)==0 && u.find("SET")!=std::string::npos && u.find("WHERE")!=std::string::npos;
}
bool SQLParser::isDeleteWhereEq(const std::string& sql){
    auto u = upper(trim(sql));
    return u.rfind("DELETE FROM",0)==0 && u.find("WHERE")!=std::string::npos;
}
bool SQLParser::isJoinEq(const std::string& sql){
    auto u = upper(trim(sql));
    return u.rfind("SELECT * FROM",0)==0 && u.find("JOIN")!=std::string::npos && u.find("ON")!=std::string::npos;
}

CreateTableStmt SQLParser::parseCreateTable(const std::string& sql){
    std::string s = trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up = upper(s);
    if(up.rfind("CREATE TABLE",0)!=0) throw std::runtime_error("Not CREATE TABLE");

    std::string rest = trim(s.substr(std::string("CREATE TABLE").size()));
    size_t paren = rest.find('(');
    if(paren==std::string::npos) throw std::runtime_error("Missing (");
    std::string tableName = trim(rest.substr(0, paren));
    std::string inside = rest.substr(paren+1);
    size_t close = inside.rfind(')');
    if(close==std::string::npos) throw std::runtime_error("Missing )");
    inside = inside.substr(0, close);

    Schema schema; schema.tableName = tableName;

    std::stringstream ss(inside);
    std::string token;
    while(std::getline(ss, token, ',')){
        token = trim(token);
        if(token.empty()) continue;
        std::stringstream cs(token);
        std::string cn, ct;
        cs >> cn >> ct;
        Column col; col.name=cn;
        ct = upper(ct);
        if(ct=="INT"||ct=="INT32") col.type=ColType::INT32;
        else if(ct=="TEXT"||ct=="STRING") col.type=ColType::TEXT;
        else throw std::runtime_error("Unknown type "+ct);
        schema.columns.push_back(col);
    }
    return CreateTableStmt{schema};
}

InsertStmt SQLParser::parseInsert(const std::string& sql){
    std::string s = trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up = upper(s);
    if(up.rfind("INSERT INTO",0)!=0) throw std::runtime_error("Not INSERT");

    std::string rest = trim(s.substr(std::string("INSERT INTO").size()));
    auto upRest = upper(rest);
    size_t vpos = upRest.find("VALUES");
    if(vpos==std::string::npos) throw std::runtime_error("Missing VALUES");
    std::string table = trim(rest.substr(0, vpos));
    std::string after = trim(rest.substr(vpos + std::string("VALUES").size()));
    if(after.front()!='(' || after.back()!=')') throw std::runtime_error("VALUES must be (...)");

    std::string inside = after.substr(1, after.size()-2);

    std::vector<Value> vals;
    std::string cur;
    bool inQuote=false;
    for(char c: inside){
        if(c=='"'){ inQuote=!inQuote; continue; }
        if(c==',' && !inQuote){
            cur=trim(cur);
            if(!cur.empty()){
                bool isNum = (std::isdigit((unsigned char)cur[0])||cur[0]=='-');
                if(isNum) vals.push_back((int32_t)std::stoi(cur));
                else vals.push_back(cur);
            }
            cur.clear();
        } else cur.push_back(c);
    }
    cur=trim(cur);
    if(!cur.empty()){
        bool isNum = (std::isdigit((unsigned char)cur[0])||cur[0]=='-');
        if(isNum) vals.push_back((int32_t)std::stoi(cur));
        else vals.push_back(cur);
    }

    return InsertStmt{table, vals};
}

SelectAllStmt SQLParser::parseSelectAll(const std::string& sql){
    std::string s = trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up=upper(s);
    if(up.rfind("SELECT * FROM",0)!=0) throw std::runtime_error("Not SELECT");
    std::string table=trim(s.substr(std::string("SELECT * FROM").size()));
    return SelectAllStmt{table};
}

static WhereEq parseWhereEqPart(const std::string& right){
    size_t eq = right.find('=');
    if(eq==std::string::npos) throw std::runtime_error("WHERE must have =");
    std::string col = trim(right.substr(0, eq));
    std::string valRaw = trim(right.substr(eq+1));

    Value v;
    if(!valRaw.empty() && valRaw.front()=='"' && valRaw.back()=='"'){
        v = valRaw.substr(1, valRaw.size()-2);
    } else {
        v = (int32_t)std::stoi(valRaw);
    }
    return WhereEq{col, v};
}

SelectWhereStmt SQLParser::parseSelectWhereEq(const std::string& sql){
    std::string s=trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up=upper(s);
    if(up.rfind("SELECT * FROM",0)!=0) throw std::runtime_error("Not SELECT");

    size_t wherePos = up.find("WHERE");
    if(wherePos==std::string::npos) throw std::runtime_error("Missing WHERE");
    std::string left = trim(s.substr(0, wherePos));
    std::string right = trim(s.substr(wherePos + 5));

    std::string table = trim(left.substr(std::string("SELECT * FROM").size()));
    return SelectWhereStmt{table, parseWhereEqPart(right)};
}

UpdateWhereStmt SQLParser::parseUpdateWhereEq(const std::string& sql){
    std::string s=trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up=upper(s);
    if(up.rfind("UPDATE",0)!=0) throw std::runtime_error("Not UPDATE");

    size_t setPos = up.find("SET");
    size_t wherePos = up.find("WHERE");
    if(setPos==std::string::npos || wherePos==std::string::npos) throw std::runtime_error("UPDATE needs SET + WHERE");
    std::string table = trim(s.substr(std::string("UPDATE").size(), setPos-std::string("UPDATE").size()));
    std::string setPart = trim(s.substr(setPos+3, wherePos-(setPos+3)));
    std::string wherePart = trim(s.substr(wherePos+5));

    auto setEq = parseWhereEqPart(setPart);
    auto whereEq = parseWhereEqPart(wherePart);

    return UpdateWhereStmt{table, setEq.col, setEq.value, whereEq};
}

DeleteWhereStmt SQLParser::parseDeleteWhereEq(const std::string& sql){
    std::string s=trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up=upper(s);
    if(up.rfind("DELETE FROM",0)!=0) throw std::runtime_error("Not DELETE");

    size_t wherePos = up.find("WHERE");
    if(wherePos==std::string::npos) throw std::runtime_error("DELETE needs WHERE");
    std::string table = trim(s.substr(std::string("DELETE FROM").size(), wherePos-std::string("DELETE FROM").size()));
    std::string wherePart = trim(s.substr(wherePos+5));
    return DeleteWhereStmt{table, parseWhereEqPart(wherePart)};
}

JoinStmt SQLParser::parseJoinEq(const std::string& sql){
    std::string s=trim(sql);
    if(!s.empty() && s.back()==';') s.pop_back();
    auto up=upper(s);
    if(up.rfind("SELECT * FROM",0)!=0) throw std::runtime_error("Not SELECT");

    size_t joinPos = up.find("JOIN");
    size_t onPos   = up.find("ON");
    if(joinPos==std::string::npos || onPos==std::string::npos) throw std::runtime_error("JOIN needs JOIN and ON");

    std::string onPart = trim(s.substr(onPos+2));

    size_t eq = onPart.find('=');
    if(eq==std::string::npos) throw std::runtime_error("ON needs =");

    std::string leftKey = trim(onPart.substr(0, eq));
    std::string rightKey = trim(onPart.substr(eq+1));

    auto dot1 = leftKey.find('.');
    auto dot2 = rightKey.find('.');
    if(dot1==std::string::npos || dot2==std::string::npos) throw std::runtime_error("Use A.col = B.col form");

    std::string leftTable = leftKey.substr(0, dot1);
    std::string leftCol = leftKey.substr(dot1+1);

    std::string rightTable = rightKey.substr(0, dot2);
    std::string rightCol = rightKey.substr(dot2+1);

    return JoinStmt{leftTable, rightTable, leftCol, rightCol};
}

}
