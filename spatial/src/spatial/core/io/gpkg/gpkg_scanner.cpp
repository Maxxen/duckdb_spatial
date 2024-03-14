
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/table.hpp"

#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"

#include "spatial/core/io/gpkg/sqlite_utils.hpp"
#include "spatial/core/io/gpkg/sqlite_db.hpp"
#include "spatial/core/io/gpkg/sqlite_stmt.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GPKG Helpers
//------------------------------------------------------------------------------
struct GPKGGeometryColumn {
    string table_name;
    string column_name;
    string geometry_type_name;
    idx_t srid;
    bool has_z;
    bool has_m;
};

static vector<GPKGGeometryColumn> GetGeometryColumns(SQLiteDB &db, const string &table_name) {
    vector<GPKGGeometryColumn> result;
    auto stmt = db.Prepare(StringUtil::Format("SELECT * FROM gpkg_geometry_columns WHERE table_name = '%s'",
                                              SQLiteUtils::SanitizeString(table_name)));
    while (stmt.Step()) {
        GPKGGeometryColumn column;
        column.table_name = stmt.GetValue<string>(0);
        column.column_name = stmt.GetValue<string>(1);
        column.geometry_type_name = stmt.GetValue<string>(2);
        column.srid = stmt.GetValue<int>(3);
        column.has_z = stmt.GetValue<int>(4) != 0;
        column.has_m = stmt.GetValue<int>(5) != 0;
        result.push_back(column);
    }
    return result;
}

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
struct GeoPackageBindData : public FunctionData {

    string file_name;
    string table_name;

    vector<string> names;
    vector<LogicalType> types;

    idx_t max_rowid = 0;
    idx_t rows_per_group = 122880;

    SQLiteDB *global_db;

    unique_ptr<FunctionData> Copy() const override {
        return nullptr; //return make_uniq<GeoPackageBindData>();
    }

    bool Equals(const FunctionData &other) const override {
        return false;
    }
};

static unique_ptr<FunctionData> GeoPackageBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &types, vector<string> &names) {
    auto result = make_uniq<GeoPackageBindData>();
    result->file_name = input.inputs[0].GetValue<string>();
    result->table_name = input.inputs[1].GetValue<string>();


    SQLiteOpenOptions options;
    options.access_mode = AccessMode::READ_ONLY;

    auto db = SQLiteDB::Open(result->file_name, options, false);

    ColumnList columns;
    vector<unique_ptr<Constraint>> constraints;
    db.GetTableInfo(result->table_name, columns, constraints, false);

    auto geometry_columns = GetGeometryColumns(db, result->table_name);
    for (const auto &col : columns.Logical()) {
        auto name = col.GetName();
        auto type = col.GetType();

        for(const auto &geometry_column : geometry_columns) {
            if (name == geometry_column.column_name) {
                type = GeoTypes::GEOMETRY();
                break;
            }
        }

        names.push_back(name);
        types.push_back(type);
    }

    if(names.empty()) {
        throw InvalidInputException("Table %s has no columns" + result->table_name);
    }

    if (!db.GetMaxRowId(result->table_name, result->max_rowid)) {
        result->max_rowid = idx_t(-1);
        result->rows_per_group = idx_t(-1);
    }

    result->names = names;
    result->types = types;

    return std::move(result);
}

//------------------------------------------------------------------------------
// Global State
//------------------------------------------------------------------------------

struct GeoPackageGlobalState : public GlobalTableFunctionState {
    mutex lock;
    idx_t position;
    idx_t max_threads;

    GeoPackageGlobalState() : position(0), max_threads(0) { }

    idx_t MaxThreads() const override {
        return max_threads;
    }
};

static unique_ptr<GlobalTableFunctionState> GeoPackageInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GeoPackageBindData>();
    auto result = make_uniq<GeoPackageGlobalState>();

    result->position = 0;
    result->max_threads = bind_data.max_rowid / bind_data.rows_per_group;

    return std::move(result);
}

//------------------------------------------------------------------------------
// Local State
//------------------------------------------------------------------------------
struct GeoPackageLocalState : public LocalTableFunctionState {
    SQLiteDB *db;
    SQLiteDB owned_db;
    SQLiteStatement stmt;
    bool done;
    vector<column_t> column_ids;

    GeometryFactory factory;
    WKBReader wkb_reader;

    explicit GeoPackageLocalState(ClientContext &context) : db(nullptr), done(false),
        factory(BufferAllocator::Get(context)), wkb_reader(factory.allocator){ }
};

static void GeoPackageInitInternal(ClientContext &context, const GeoPackageBindData &bind_data, GeoPackageLocalState &lstate, idx_t start, idx_t end) {
    D_ASSERT(start <= end);

    lstate.done = false;
    // we may have leftover statements or connections from a previous call to this function
    lstate.stmt.Close();
    if (!lstate.db) {
        SQLiteOpenOptions options;
        options.access_mode = AccessMode::READ_ONLY;
        lstate.owned_db = SQLiteDB::Open(bind_data.file_name, options);
        lstate.db = &lstate.owned_db;
    }

    auto col_names = StringUtil::Join(lstate.column_ids.data(), lstate.column_ids.size(), ", ", [&](const idx_t column_id) {
        if(column_id == column_t(-1)) {
            return string { "ROWID" };
        } else {
            return '"' + SQLiteUtils::SanitizeIdentifier(bind_data.names[column_id]) + '"';
        }
    });

    auto sql = StringUtil::Format("SELECT %s FROM \"%s\"", col_names, SQLiteUtils::SanitizeIdentifier(bind_data.table_name));
    if (bind_data.rows_per_group != idx_t(-1)) {
        // we are scanning a subset of the rows - generate a WHERE clause based on
        // the rowid
        auto where_clause = StringUtil::Format(" WHERE ROWID BETWEEN %d AND %d", start, end);
        sql += where_clause;
    } else {
        // we are scanning the entire table - no need for a WHERE clause
        D_ASSERT(start == 0);
    }
    lstate.stmt = lstate.db->Prepare(sql);
}

static bool GeoPackageParallelStateNext(ClientContext &context, const GeoPackageBindData &bind_data, GeoPackageLocalState &lstate, GeoPackageGlobalState &gstate) {
    lock_guard<mutex> parallel_lock(gstate.lock);
    if (gstate.position < bind_data.max_rowid) {
        auto start = gstate.position;
        auto end = start + bind_data.rows_per_group - 1;
        GeoPackageInitInternal(context, bind_data, lstate, start, end);
        gstate.position = end + 1;
        return true;
    }
    return false;
}

static unique_ptr<LocalTableFunctionState> GeoPackageInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state_p) {
    auto &bind_data = input.bind_data->Cast<GeoPackageBindData>();
    auto &gstate = global_state_p->Cast<GeoPackageGlobalState>();
    auto result = make_uniq<GeoPackageLocalState>(context.client);
    result->column_ids = input.column_ids;
    result->db = bind_data.global_db;
    if (!GeoPackageParallelStateNext(context.client, bind_data, *result, gstate)) {{
        result->done = true;
    }}

    return std::move(result);
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------

static void ConvertGeometryColumn(Vector &out_vec, idx_t out_idx, sqlite3_value *val, WKBReader &reader, GeometryFactory &factory) {
    auto ptr = data_ptr_cast(const_cast<void*>(sqlite3_value_blob(val)));
    auto size = sqlite3_value_bytes(val);

    Cursor cursor(ptr, ptr + size);
    auto magic1 = cursor.Read<uint8_t>();
    auto magic2 = cursor.Read<uint8_t>();
    if(magic1 != 'G' || magic2 != 'P') {
        throw InvalidInputException("Invalid GPKG magic bytes");
    }

    auto version = cursor.Read<uint8_t>();
    auto flags = cursor.Read<uint8_t>();
    auto srs_id = cursor.Read<int32_t>();

    // TODO: Check how large envelope is based on bit flags
    // TODO: Check endianness flags before reading envelope, SRID, etc.
    // For now assume 32 bytes
    cursor.Skip(32);

    auto geom = reader.Deserialize(cursor.GetPtr(), cursor.Remaining());
    FlatVector::GetData<string_t>(out_vec)[out_idx] = factory.Serialize(out_vec, geom, reader.GeomHasZ(), reader.GeomHasM());
}

static void GeoPackageScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &lstate = data.local_state->Cast<GeoPackageLocalState>();
    auto &gstate = data.global_state->Cast<GeoPackageGlobalState>();
    auto &bind_data = data.bind_data->Cast<GeoPackageBindData>();

    while (output.size() == 0) {
        if (lstate.done) {
            if (!GeoPackageParallelStateNext(context, bind_data, lstate, gstate)) {
                return;
            }
        }

        idx_t out_idx = 0;
        while (true) {
            if (out_idx == STANDARD_VECTOR_SIZE) {
                output.SetCardinality(out_idx);
                return;
            }
            auto &stmt = lstate.stmt;
            auto has_more = stmt.Step();
            if (!has_more) {
                lstate.done = true;
                output.SetCardinality(out_idx);
                break;
            }
            for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
                auto &out_vec = output.data[col_idx];
                auto sqlite_column_type = stmt.GetType(col_idx);
                if (sqlite_column_type == SQLITE_NULL) {
                    auto &mask = FlatVector::Validity(out_vec);
                    mask.Set(out_idx, false);
                    continue;
                }

                auto val = stmt.GetValue<sqlite3_value *>(col_idx);
                auto ltype = out_vec.GetType();
                switch (ltype.id()) {
                    case LogicalTypeId::BIGINT:
                        stmt.CheckTypeMatches(false, val, sqlite_column_type, SQLITE_INTEGER, col_idx);
                        FlatVector::GetData<int64_t>(out_vec)[out_idx] = sqlite3_value_int64(val);
                        break;
                    case LogicalTypeId::DOUBLE:
                        stmt.CheckTypeIsFloatOrInteger(val, sqlite_column_type, col_idx);
                        FlatVector::GetData<double>(out_vec)[out_idx] = sqlite3_value_double(val);
                        break;
                    case LogicalTypeId::VARCHAR:
                        stmt.CheckTypeMatches(false, val, sqlite_column_type, SQLITE_TEXT, col_idx);
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = StringVector::AddString(
                                out_vec, (const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
                        break;
                    case LogicalTypeId::DATE:
                        stmt.CheckTypeMatches(false, val, sqlite_column_type, SQLITE_TEXT, col_idx);
                        FlatVector::GetData<date_t>(out_vec)[out_idx] =
                                Date::FromCString((const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
                        break;
                    case LogicalTypeId::TIMESTAMP:
                        stmt.CheckTypeMatches(false, val, sqlite_column_type, SQLITE_TEXT, col_idx);
                        FlatVector::GetData<timestamp_t>(out_vec)[out_idx] =
                                Timestamp::FromCString((const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
                        break;
                    case LogicalTypeId::BLOB:
                        if(ltype == GeoTypes::GEOMETRY()) {
                            ConvertGeometryColumn(out_vec, out_idx, val, lstate.wkb_reader, lstate.factory);
                        } else {
                            FlatVector::GetData<string_t>(out_vec)[out_idx] = StringVector::AddStringOrBlob(
                                    out_vec, (const char *) sqlite3_value_blob(val), sqlite3_value_bytes(val));
                        }
                        break;
                    default:
                        throw std::runtime_error(out_vec.GetType().ToString());
                }
            }
            out_idx++;
        }
    }
}

//------------------------------------------------------------------------------
// Misc
//------------------------------------------------------------------------------

static double GeoPackageProgress(ClientContext &context, const FunctionData *bind_data_p, const GlobalTableFunctionState *global_state_p) {
    auto &bind_data = bind_data_p->Cast<GeoPackageBindData>();
    auto &global_state = global_state_p->Cast<GeoPackageGlobalState>();
    return static_cast<double>(global_state.position) / static_cast<double>(bind_data.max_rowid);
}

static unique_ptr<NodeStatistics> GeoPackageCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    D_ASSERT(bind_data_p);
    auto &bind_data = bind_data_p->Cast<GeoPackageBindData>();
    return make_uniq<NodeStatistics>(bind_data.max_rowid);
}

void CoreTableFunctions::RegisterGeoPackageTableFunction(DatabaseInstance &db) {

    TableFunction read_func("ST_ReadGPKG", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                            GeoPackageScan, GeoPackageBind, GeoPackageInitGlobal, GeoPackageInitLocal);

    read_func.table_scan_progress = GeoPackageProgress;
    read_func.cardinality = GeoPackageCardinality;
    read_func.projection_pushdown = true;
    ExtensionUtil::RegisterFunction(db, read_func);
}

}

}