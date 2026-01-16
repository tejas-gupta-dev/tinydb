#include "DBEngine.h"
#include "SQLParser.h"
#include "TableFile.h"
#include "TableScanner.h"
#include "RowCodec.h"

#include <sstream>
#include <unordered_map>

namespace tinydb {

DBEngine::DBEngine(const std::string& dbDir)
    : catalog(dbDir), wal(dbDir + "/db.wal") {}

std::string DBEngine::jsonEscape(const std::string& s) const {
    std::string out;
    for(char c: s){
        if(c=='\\') out += "\\\\";
        else if(c=='"') out += "\\\"";
        else if(c=='\n') out += "\\n";
        else out += c;
    }
    return out;
}

int DBEngine::colIndex(const Schema& schema, const std::string& col) const{
    for(size_t i=0;i<schema.columns.size();i++){
        if(schema.columns[i].name==col) return (int)i;
    }
    return -1;
}

void DBEngine::buildIndexIfMissing(const std::string& table, const Schema& schema){
    if(indexes.find(table)!=indexes.end()) return;
    rebuildIndexes(table, schema);
}

void DBEngine::rebuildIndexes(const std::string& table, const Schema& schema){
    indexes[table] = {};
    for(auto& c: schema.columns){
        indexes[table][c.name] = HashIndex{};
    }

    TableFile tf(catalog.tablePath(table));
    TableScanner sc(tf);
    auto rows = sc.scanAll();

    for(auto& r: rows){
        auto vals = RowCodec::decode(schema, r.bytes);
        for(size_t i=0;i<schema.columns.size();i++){
            indexes[table][schema.columns[i].name].add(RowCodec::valueToKey(vals[i]), r.rid);
        }
    }
}

std::string DBEngine::execute(const std::string& sql){
    try{
        if(SQLParser::isCreateTable(sql)){
            auto stmt = SQLParser::parseCreateTable(sql);
            bool ok = catalog.createTable(stmt.schema);
            return ok ? R"({"ok":true,"msg":"table created"})" : R"({"ok":false,"msg":"create failed"})";
        }

        if(SQLParser::isInsert(sql)){
            auto stmt = SQLParser::parseInsert(sql);
            if(!catalog.hasTable(stmt.table)) return R"({"ok":false,"msg":"table not found"})";
            Schema schema = catalog.loadSchema(stmt.table);

            auto rowBytes = RowCodec::encode(schema, stmt.values);
            wal.logInsert(stmt.table, rowBytes);

            TableFile tf(catalog.tablePath(stmt.table));
            RowId rid = tf.insertRow(rowBytes);

            buildIndexIfMissing(stmt.table, schema);
            auto vals = RowCodec::decode(schema, rowBytes);
            for(size_t i=0;i<schema.columns.size();i++){
                indexes[stmt.table][schema.columns[i].name].add(RowCodec::valueToKey(vals[i]), rid);
            }

            std::ostringstream oss;
            oss << R"({"ok":true,"msg":"inserted","page":)" << rid.pageId << R"(,"slot":)" << rid.slotId << "}";
            return oss.str();
        }

        if(SQLParser::isJoinEq(sql)){
            auto stmt = SQLParser::parseJoinEq(sql);
            if(!catalog.hasTable(stmt.leftTable) || !catalog.hasTable(stmt.rightTable))
                return R"({"ok":false,"msg":"join table not found"})";

            Schema leftSchema = catalog.loadSchema(stmt.leftTable);
            Schema rightSchema = catalog.loadSchema(stmt.rightTable);

            int lci = colIndex(leftSchema, stmt.leftCol);
            int rci = colIndex(rightSchema, stmt.rightCol);
            if(lci<0 || rci<0) return R"({"ok":false,"msg":"join columns not found"})";

            TableFile ltf(catalog.tablePath(stmt.leftTable));
            TableFile rtf(catalog.tablePath(stmt.rightTable));
            TableScanner lsc(ltf), rsc(rtf);

            auto lrows = lsc.scanAll();
            auto rrows = rsc.scanAll();

            std::unordered_map<std::string, std::vector<std::string>> mapR;
            for(auto& rr: rrows){
                auto rv = RowCodec::decode(rightSchema, rr.bytes);
                std::string key = RowCodec::valueToKey(rv[rci]);
                mapR[key].push_back(RowCodec::toString(rightSchema, rv));
            }

            std::ostringstream oss;
            oss << R"({"ok":true,"rows":[)";
            bool first=true;

            for(auto& lr: lrows){
                auto lv = RowCodec::decode(leftSchema, lr.bytes);
                std::string key = RowCodec::valueToKey(lv[lci]);

                auto it = mapR.find(key);
                if(it==mapR.end()) continue;

                std::string leftStr = RowCodec::toString(leftSchema, lv);
                for(auto& rightStr : it->second){
                    if(!first) oss << ",";
                    first=false;
                    oss << R"({"left":")" << jsonEscape(leftStr) << R"(","right":")" << jsonEscape(rightStr) << R"("})";
                }
            }
            oss << "]}";
            return oss.str();
        }

        if(SQLParser::isSelectWhereEq(sql)){
            auto stmt = SQLParser::parseSelectWhereEq(sql);
            if(!catalog.hasTable(stmt.table)) return R"({"ok":false,"msg":"table not found"})";
            Schema schema = catalog.loadSchema(stmt.table);
            buildIndexIfMissing(stmt.table, schema);

            std::string key = RowCodec::valueToKey(stmt.where.value);

            auto it = indexes[stmt.table].find(stmt.where.col);
            if(it==indexes[stmt.table].end()) return R"({"ok":false,"msg":"col not found"})";

            auto rids = it->second.find(key);
            TableFile tf(catalog.tablePath(stmt.table));

            std::ostringstream oss;
            oss << R"({"ok":true,"rows":[)";
            bool first=true;
            for(auto& rid: rids){
                auto bytes=tf.readRow(rid);
                if(bytes.empty()) continue;
                auto vals=RowCodec::decode(schema, bytes);
                if(!first) oss << ",";
                first=false;
                oss << R"({"rid":{"page":)"<<rid.pageId<<R"(,"slot":)"<<rid.slotId<<R"(},"data":")"
                    << jsonEscape(RowCodec::toString(schema, vals)) << R"("})";
            }
            oss << "]}";
            return oss.str();
        }

        if(SQLParser::isSelectAll(sql)){
            auto stmt = SQLParser::parseSelectAll(sql);
            if(!catalog.hasTable(stmt.table)) return R"({"ok":false,"msg":"table not found"})";
            Schema schema = catalog.loadSchema(stmt.table);
            TableFile tf(catalog.tablePath(stmt.table));
            TableScanner sc(tf);
            auto rows=sc.scanAll();

            std::ostringstream oss;
            oss << R"({"ok":true,"rows":[)";
            bool first=true;
            for(auto& r: rows){
                auto vals=RowCodec::decode(schema, r.bytes);
                if(!first) oss << ",";
                first=false;
                oss << R"({"rid":{"page":)"<<r.rid.pageId<<R"(,"slot":)"<<r.rid.slotId<<R"(},"data":")"
                    << jsonEscape(RowCodec::toString(schema, vals)) << R"("})";
            }
            oss << "]}";
            return oss.str();
        }

        if(SQLParser::isUpdateWhereEq(sql)){
            auto stmt = SQLParser::parseUpdateWhereEq(sql);
            if(!catalog.hasTable(stmt.table)) return R"({"ok":false,"msg":"table not found"})";
            Schema schema = catalog.loadSchema(stmt.table);
            buildIndexIfMissing(stmt.table, schema);

            int setIdx = colIndex(schema, stmt.setCol);
            int whereIdx = colIndex(schema, stmt.where.col);
            if(setIdx<0 || whereIdx<0) return R"({"ok":false,"msg":"column not found"})";

            TableFile tf(catalog.tablePath(stmt.table));

            auto rids = indexes[stmt.table][stmt.where.col].find(RowCodec::valueToKey(stmt.where.value));

            int updated=0;
            for(auto& rid: rids){
                auto bytes = tf.readRow(rid);
                if(bytes.empty()) continue;

                auto vals = RowCodec::decode(schema, bytes);
                if(RowCodec::valueToKey(vals[whereIdx]) != RowCodec::valueToKey(stmt.where.value)) continue;

                vals[setIdx] = stmt.setValue;
                auto newBytes = RowCodec::encode(schema, vals);

                bool ok = tf.updateRow(rid, newBytes);
                if(ok){
                    wal.logUpdate(stmt.table, rid.pageId, rid.slotId, newBytes);
                    updated++;
                }
            }

            rebuildIndexes(stmt.table, schema);

            std::ostringstream oss;
            oss << R"({"ok":true,"updated":)" << updated << "}";
            return oss.str();
        }

        if(SQLParser::isDeleteWhereEq(sql)){
            auto stmt = SQLParser::parseDeleteWhereEq(sql);
            if(!catalog.hasTable(stmt.table)) return R"({"ok":false,"msg":"table not found"})";
            Schema schema = catalog.loadSchema(stmt.table);
            buildIndexIfMissing(stmt.table, schema);

            TableFile tf(catalog.tablePath(stmt.table));

            auto rids = indexes[stmt.table][stmt.where.col].find(RowCodec::valueToKey(stmt.where.value));

            int deleted=0;
            for(auto& rid: rids){
                auto bytes=tf.readRow(rid);
                if(bytes.empty()) continue;
                bool ok=tf.deleteRow(rid);
                if(ok){
                    wal.logDelete(stmt.table, rid.pageId, rid.slotId);
                    deleted++;
                }
            }

            rebuildIndexes(stmt.table, schema);

            std::ostringstream oss;
            oss << R"({"ok":true,"deleted":)" << deleted << "}";
            return oss.str();
        }

        return R"({"ok":false,"msg":"unsupported SQL"})";
    } catch(const std::exception& e){
        return std::string(R"({"ok":false,"msg":")") + jsonEscape(e.what()) + R"("})";
    }
}

}
