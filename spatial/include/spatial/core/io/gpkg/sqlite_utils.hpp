//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "spatial/common.hpp"
#include "sqlite3.h"

namespace spatial {

namespace core {

class SQLiteUtils {
public:
    static void Check(int rc, sqlite3 *db);
    static string TypeToString(int sqlite_type);
    static LogicalType TypeToLogicalType(const string &sqlite_type);
    static string SanitizeString(const string &table_name);
    static string SanitizeIdentifier(const string &table_name);
    static LogicalType ToSQLiteType(const LogicalType &input);
    string ToSQLiteTypeAlias(const LogicalType &input);
};

} // namespace core

} // namespace spatial