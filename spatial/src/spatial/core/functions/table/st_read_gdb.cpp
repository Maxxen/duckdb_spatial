#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

#include <locale>
#include <codecvt>
#include <cmath>

namespace spatial {

namespace core {


struct int40_t {
    uint32_t low;
    int8_t high;

    int64_t ToInt64() {
        return ((int64_t)high << 32) | (int64_t)low;
    }

    operator int64_t() {
        return ToInt64();
    }
};

struct int48_t {
    uint32_t low;
    int16_t high;

    int64_t ToInt64() {
        return ((int64_t)high << 32) | (int64_t)low;
    }

    operator int64_t() {
        return ToInt64();
    }
};

struct BinaryReader {
    data_ptr_t ptr;
    data_ptr_t end;
    BinaryReader(data_ptr_t ptr, idx_t size) : ptr(ptr), end(ptr + size) { }
    BinaryReader(data_ptr_t ptr, data_ptr_t end) : ptr(ptr), end(end) { D_ASSERT(ptr <= end); }

    idx_t BytesRead() {
        return end - ptr;
    }

    void Skip(idx_t bytes) {
        if(ptr + bytes > end) {
            throw SerializationException("Skipped past end of buffer");
        }
        ptr += bytes;
    }

    template <class T>
    T Read() {
        if(ptr + sizeof(T) > end) {
            throw SerializationException("Read past end of buffer");
        }
        auto value = Load<T>(ptr);
        ptr += sizeof(T);
        return value;
    }

    // MSB variable size int encoding
    uint64_t ReadUnsignedVarint(idx_t *bytes_read = nullptr) {
        uint64_t value = 0;
        idx_t offset = 0;
        uint8_t byte;
        do {
            byte = Read<uint8_t>();
            value |= (uint64_t)(byte & 0x7F) << (offset * 7);
            offset++;
        } while(byte & 0x80);
        if(bytes_read) {
            *bytes_read = offset;
        }
        return value;
    }

    int64_t ReadSignedVarint(idx_t *bytes_read = nullptr) {
        uint64_t value = 0;
        idx_t offset = 0;
        uint8_t byte;
        do {
            byte = Read<uint8_t>();
            value |= (uint64_t)(byte & 0x7F) << (offset * 7);
            offset++;
        } while(byte & 0x80);
        if(bytes_read) {
            *bytes_read = offset;
        }
        return (value >> 1) ^ -(value & 1);
    }

    void ReadData(void* destination, idx_t size) {
        if(ptr + size > end) {
            throw SerializationException("Read past end of buffer");
        }
        memcpy(destination, ptr, size);
        ptr += size;
    }

    template<class T = char>
    std::basic_string<T> ReadString(idx_t size) {
        std::basic_string<T> str;
        str.resize(size);
        ReadData((void*)str.data(), size * sizeof(T));
        return str;
    }
};


std::string utf16_to_utf8(std::u16string const& s)
{
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t, 0x10ffff,
        std::codecvt_mode::little_endian>, char16_t> cnv;
    std::string utf8 = cnv.to_bytes(s);
    if(cnv.converted() < s.size())
        throw std::runtime_error("incomplete conversion");
    return utf8;
}

uint64_t read_varuint(const_data_ptr_t ptr, idx_t *read) {
    uint64_t value = 0;
    idx_t offset = 0;
    uint8_t byte;
    do {
        byte = Load<uint8_t>(ptr + offset);
        value |= (uint64_t)(byte & 0x7F) << (offset * 7);
        offset++;
    } while (byte & 0x80);
    *read = offset;
    return value;
}

//------------------------------------------------------------------------------
// BIND
//------------------------------------------------------------------------------
struct FieldDef {
    string name;
    LogicalType type;
    bool has_null;
    bool has_default;
};

struct BindData : TableFunctionData {
	string file_name;
    unique_ptr<FileHandle> handle;
    vector<FieldDef> fields;
    idx_t nullable_fields_count;

	BindData(string file_name, unique_ptr<FileHandle> handle, vector<FieldDef> fields, idx_t nullable_fields_count) : 
        file_name(file_name),
        handle(std::move(handle)),
        fields(std::move(fields)),
        nullable_fields_count(nullable_fields_count) {
    }
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {

    auto file_name = StringValue::Get(input.inputs[0]);
    auto &fs = FileSystem::GetFileSystem(context);
	auto opener = FileSystem::GetFileOpener(context);
    auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK, FileCompressionType::UNCOMPRESSED, opener);
    auto file_size = handle->GetFileSize();

    // Now figure out the schema
    uint8_t header[40];
    handle->Read(header, 40, 0);
    BinaryReader hreader(header, 40);

    auto gdb_magic = hreader.Read<int32_t>(); // magic
    auto gdb_valid_rows = hreader.Read<int32_t>(); // valid rows
    auto gdb_row_size = hreader.Read<int32_t>(); // row size
    
    auto gdb_unknown_1 = hreader.Read<int32_t>(); // unknown
    auto gdb_unknown_2 = hreader.Read<int32_t>(); // unknown
    auto gdb_unknown_3 = hreader.Read<int32_t>(); // unknown

    auto gdb_file_size = hreader.Read<int64_t>(); // file size
    auto gdb_field_description_start_offset = hreader.Read<int64_t>(); // offset in bytes at which the data starts
    
    // field descriptor (fixed part)
    // 4 + 4 + 4 + 2
    uint8_t field_descriptor_fixed[14];
    handle->Read(field_descriptor_fixed, 14, gdb_field_description_start_offset);
    BinaryReader fdreader(field_descriptor_fixed, 14);

    auto gdb_field_size = fdreader.Read<int32_t>(); // not including the field itself;
    auto gdb_version = fdreader.Read<int32_t>();
    auto gdb_layer_flags = fdreader.Read<uint32_t>();
    auto gdb_field_count =fdreader.Read<int16_t>();

    bool has_utf8 = (1 << 8) & gdb_layer_flags;
    bool layer_has_m = (1 << 30) & gdb_layer_flags;
    bool layer_has_z = (1 << 31) & gdb_layer_flags;

    if(!has_utf8) {
        throw BinderException("Only UTF8 is supported");
    }
    if (layer_has_m) {
        throw BinderException("M is not supported");
    }
    if (layer_has_z) {
        throw BinderException("Z is not supported");
    }

    // TODO: Remove these
    names.push_back("size");
    return_types.push_back(LogicalType::BIGINT);

    uint8_t geom_type = gdb_layer_flags & 0x000000FF;
    switch (geom_type) {
        case 0: names.push_back("None"); break;
        case 1: names.push_back("Point"); break;
        case 2: names.push_back("MultiPoint"); break;
        case 3: names.push_back("MultiLineString"); break;
        case 4: names.push_back("MultiPolygon"); break;
        default:
            names.push_back(StringUtil::Format("Unknown: %d", geom_type));
        break;
    }
    return_types.push_back(LogicalType::VARCHAR);

    vector<uint8_t> fields_buffer(gdb_field_size);
    handle->Read(fields_buffer.data(), gdb_field_size, gdb_field_description_start_offset + 14);
    vector<FieldDef> fields;
    idx_t nullable_fields_count = 0;

    // Field reader
    BinaryReader freader(fields_buffer.data(), gdb_field_size);

    // field descriptor (variable part)    
    for(idx_t field_idx = 0; field_idx < gdb_field_count; field_idx++) {
        
        // This says "amount of utf16 characters, not bytes", but I assume its codepoints?
        auto utf16_field_len = freader.Read<uint8_t>();
        auto field_name_utf16 = freader.ReadString<char16_t>(utf16_field_len);
        auto field_name = utf16_to_utf8(field_name_utf16);
        names.push_back(field_name);

        // skip the alias
        auto utf16_alias_len = freader.Read<uint8_t>();
        freader.Skip(utf16_alias_len * sizeof(char16_t));

        auto field_type = freader.Read<uint8_t>();

        switch(field_type)
        {
            case 4: { // string
                auto max_length = freader.Read<int32_t>();
                auto flag = freader.Read<uint8_t>();

                bool has_null = (flag & 1) != 0;
                bool has_default = (flag & 4) != 0;

                auto default_value_len = freader.ReadUnsignedVarint();
                if(has_default) {
                    freader.Skip(default_value_len); // default value, skip for now
                }
                return_types.push_back(LogicalType::VARCHAR);
                fields.push_back({field_name, LogicalType::VARCHAR, has_null, has_default});
                if(has_null) {
                    nullable_fields_count++;
                }

            }
            break;
            case 6: { // objectID
                auto width = freader.Read<uint8_t>();
                auto flag = freader.Read<uint8_t>();

                bool has_null = (flag & 1) != 0;
                return_types.push_back(LogicalType::BIGINT);
                fields.push_back({field_name, LogicalType::BIGINT, false, false});
                if(has_null) {
                    nullable_fields_count++;
                }

            } break;

            case 7: // geometry
            {
                freader.Skip(1); // unknown
                auto flag = freader.Read<uint8_t>();
                bool has_null = (flag & 1) != 0;

                auto srid_length_bytes = freader.Read<int16_t>();

                // this is utf8 encoded
                auto wkt_srs = freader.ReadString(srid_length_bytes);
    
                uint8_t flags = freader.Read<uint8_t>();
                bool has_z = (flags & (1 << 1)) != 0;
                bool has_m = (flags & (1 << 2)) != 0;
                if (has_z) {
                    //throw BinderException("Geometry Z is not supported");
                }
                if (has_m) {
                    //throw BinderException("Geometry M is not supported");
                }

                auto x_origin = freader.Read<double>();
                auto y_origin = freader.Read<double>();
                auto xy_scale = freader.Read<double>();
                if(has_m) {
                    auto m_origin = freader.Read<double>();
                    auto m_scale = freader.Read<double>();
                }
                if(has_z) {
                    auto z_origin = freader.Read<double>();
                    auto z_scale = freader.Read<double>();
                }
                auto xy_tolerance = freader.Read<double>();
                if(has_m) {
                    auto m_tolerance = freader.Read<double>();
                }
                if (has_z) {
                    auto z_tolerance = freader.Read<double>();
                }

                auto x_min = freader.Read<double>();
                auto y_min = freader.Read<double>();
                auto x_max = freader.Read<double>();
                auto y_max = freader.Read<double>();

                if(layer_has_z) {
                    auto z_min = freader.Read<double>();
                    auto z_max = freader.Read<double>();
                }
                if(layer_has_m) {
                    auto m_min = freader.Read<double>();
                    auto m_max = freader.Read<double>();
                }

                // spatial index
                freader.Skip(1); // unknown byte
                auto grid_size = freader.Read<uint32_t>();
                D_ASSERT(grid_size == 1 || grid_size == 2 || grid_size == 3);
                for (uint32_t i = 0; i < grid_size; i++) {
                    freader.Skip(8); // we dont care about the grid
                }

                return_types.push_back(GeoTypes::GEOMETRY());
                fields.push_back({field_name, GeoTypes::GEOMETRY(), has_null, false});
                if(has_null) {
                    nullable_fields_count++;
                }
            }
            break;

            case 8: {// binary
                auto width = freader.Read<uint8_t>();
                auto flag = freader.Read<uint8_t>();
                bool has_null = (flag & 1) != 0;
                return_types.push_back(LogicalType::BLOB);
                fields.push_back({field_name, LogicalType::BLOB, false, false});
                if(has_null) {
                    nullable_fields_count++;
                }
            } break;
            case 9: { // raster
                throw BinderException("Raster is not supported");
            } break;

            case 10: {
                auto width = freader.Read<uint8_t>();
                auto flag = freader.Read<uint8_t>();
                bool has_null = (flag & 1) != 0;
                return_types.push_back(LogicalType::VARCHAR);
                fields.push_back({field_name, LogicalType::VARCHAR, false, false});
                if(has_null) {
                    nullable_fields_count++;
                }
            } break;
            case 11: {
                auto width = freader.Read<uint8_t>();
                auto flag = freader.Read<uint8_t>();
                bool has_null = (flag & 1) != 0;
                return_types.push_back(LogicalType::VARCHAR);
                fields.push_back({field_name, LogicalType::VARCHAR, false, false});
                if(has_null) {
                    nullable_fields_count++;
                }
            } break;

            case 12: { // Unknown?
                auto width = freader.Read<uint8_t>();
                auto flag = freader.Read<uint8_t>();
                bool has_null = (flag & 1) != 0;
                return_types.push_back(LogicalType::VARCHAR);
                fields.push_back({field_name, LogicalType::VARCHAR, false, false});
                if(has_null) {
                    nullable_fields_count++;
                }
            } break;
            default: { // other
                LogicalType type;
                switch(field_type) {
                    case 0: { 
                        type = LogicalType::SMALLINT;
                        break;
                    }
                    case 1: {
                        type = LogicalType::INTEGER;
                        break;
                    }
                    case 2: {
                        type = LogicalType::FLOAT;
                        break;
                    }
                    case 3: {
                        type = LogicalType::DOUBLE;
                        break;
                    }
                    case 5: {
                        type = LogicalType::TIMESTAMP;
                        throw NotImplementedException("TIMESTAMP");
                        break;
                    }
                    case 12: {
                        // XML
                        type = LogicalType::VARCHAR;
                        throw NotImplementedException("XML");
                        break;
                    }
                    default: {
                        throw BinderException(StringUtil::Format("Unknown field type: %d", field_type));
                    }
                }
                auto width = freader.Read<uint8_t>();
                auto flag = freader.Read<uint8_t>();
                bool has_null = (flag & 1) != 0;
                bool has_default = (flag & 4) != 0;
                auto ldf = freader.Read<uint8_t>();
                if(has_default) { // if has default value
                    freader.Skip(ldf); // default value (skip for now)
                }
                return_types.push_back(type);
                fields.push_back({field_name, type, has_null, false});
                if(has_null) {
                    nullable_fields_count++;
                }
            }
            break;
        }
    }

	return make_unique<BindData>(file_name, std::move(handle), std::move(fields), nullable_fields_count);
}

//------------------------------------------------------------------------------
// GLOBAL
//------------------------------------------------------------------------------
struct GlobalState : public GlobalTableFunctionState {
    
    mutex lock;
    unique_ptr<FileHandle> handle;
    idx_t file_size;
    idx_t max_threads;
    // The size of each offset (in bytes, 4, 5 or 6
    int32_t offset_size;
    // The number of offsets
    idx_t offset_count;
    // The current offset index
    idx_t offset_idx;
    // The offsets file handle
    unique_ptr<FileHandle> offsets_handle;
    
    // Batch index
    idx_t batch_idx = 0;

    GlobalState(unique_ptr<FileHandle> handle_p, idx_t max_threads, int32_t offset_size, idx_t offset_count, unique_ptr<FileHandle> offsets_handle) : 
        handle(std::move(handle_p)),
        file_size(handle->GetFileSize()),
        max_threads(max_threads),
        offset_size(offset_size),
        offset_count(offset_count),
        offset_idx(0),
        offsets_handle(std::move(offsets_handle)),
        batch_idx(0) {
    }

    double GetProgress() {
        return 100 * ((double) offset_idx / (double) offset_count);
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}
};


static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = (BindData &)*input.bind_data;

    // Create the offset index.
    auto offsets_file = StringUtil::Replace(bind_data.file_name, ".gdbtable", ".gdbtablx");
    
    auto &fs = FileSystem::GetFileSystem(context);
	auto opener = FileSystem::GetFileOpener(context);
    auto offset_handle = fs.OpenFile(offsets_file, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK, FileCompressionType::UNCOMPRESSED, opener);
    
    // Read the header
    data_t header[16];
    offset_handle->Read(header, 16, 0);
    data_ptr_t header_ptr = (data_ptr_t)header;
    header_ptr += 4; //unknown
    int32_t num_1024_blocks = Load<int32_t>(header_ptr);
    header_ptr += 4; // num 1024 blocks
    int32_t num_rows = Load<int32_t>(header_ptr);
    header_ptr += 4; // num rows
    int32_t offset_size = Load<int32_t>(header_ptr);
    
    //D_ASSERT(offset_size == 4); // only support up to 4gb now
    //vector<int32_t> offsets(num_rows);
    //offset_handle->Read(offsets.data(), num_rows * offset_size, 16);
    

    auto max_threads = context.db->NumberOfThreads();
    return make_unique<GlobalState>(std::move(bind_data.handle), max_threads, offset_size, num_rows, std::move(offset_handle));
}

//------------------------------------------------------------------------------
// LOCAL
//------------------------------------------------------------------------------

struct LocalState : LocalTableFunctionState {

    ArenaAllocator arena;
    // len(offset_data) = num_rows * offset_size
    vector<data_t> offset_data;
    // How big one offset is: 4, 5, or 6 bytes;
    int32_t offset_size;

    unique_ptr<FileHandle> handle;

    idx_t row_count;
    idx_t current_row;

    // The current batch index
    idx_t batch_index;

    int64_t GetOffsetForRow(idx_t row) {
        switch(offset_size) {
            case 4: {
                return Load<int32_t>(offset_data.data() + row * offset_size);
            }
            case 5: {
                auto int40 = Load<int40_t>(offset_data.data() + row * offset_size);
                return int40.ToInt64();
            }
            case 6: {
                auto int48 = Load<int48_t>(offset_data.data() + row * offset_size);
                return int48.ToInt64();
            }
            default: {
                throw InternalException("Offset size %d not supported", offset_size);
            }
        }
    }

    LocalState(Allocator &alloc) : 
        arena(alloc),
        offset_data(),
        offset_size(0),
        row_count(0),
        current_row(0),
        batch_index(0) {
    }
};


static bool GetNextOffsetChunk(ClientContext& context, GlobalState& gstate, LocalState &lstate) {
    // TODO: Count number of nullable fields
    lock_guard<mutex> glock(gstate.lock);

    // Read up to 2048 offsets at a time (the standard vector size)
    idx_t offsets_to_read = std::min((idx_t)STANDARD_VECTOR_SIZE, gstate.offset_count - gstate.offset_idx);
    if (offsets_to_read == 0) {
        // No more offsets, we are done
        return true;
    }
    idx_t offsets_size = offsets_to_read * gstate.offset_size;

    // Read the offsets into the local state
    lstate.current_row = 0;
    lstate.row_count = offsets_to_read;
    lstate.offset_size = gstate.offset_size;
    lstate.offset_data = vector<data_t>(offsets_size);
    lstate.batch_index = gstate.batch_idx;

    gstate.offsets_handle->Read(lstate.offset_data.data(), offsets_size, 16 + (gstate.offset_idx * gstate.offset_size));
    gstate.offset_idx += offsets_to_read;
    gstate.batch_idx++;

    return false;
}

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
    auto &bind_data = (BindData &)*input.bind_data;
    auto &global = (GlobalState &)*global_state;
    auto local = make_unique<LocalState>(BufferAllocator::Get(context.client));

    // Open the data file for this thread
    auto &fs = FileSystem::GetFileSystem(context.client);
	auto opener = FileSystem::GetFileOpener(context.client);
    local->handle = fs.OpenFile(bind_data.file_name, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK, FileCompressionType::UNCOMPRESSED, opener);
    
    auto is_done = GetNextOffsetChunk(context.client, global, *local);
    if(is_done) {
        return nullptr;
    }

    return local;
}

//------------------------------------------------------------------------------
// EXECUTE
//------------------------------------------------------------------------------
static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    if (input.local_state == nullptr) {
        return;
    }

    auto &lstate = (LocalState &)*input.local_state;
    auto &gstate = (GlobalState &)*input.global_state;
    auto &bind_data = (BindData &)*input.bind_data;

    // Reset the arena allocator
    lstate.arena.Reset();

    // The number of bytes needed to store the nullmask for the nullable fields in each row
    idx_t nullmask_size = std::ceil(bind_data.nullable_fields_count / 8.0);

    auto rows_to_add = std::min((idx_t)STANDARD_VECTOR_SIZE, lstate.row_count - lstate.current_row);
    if (rows_to_add == 0) {
        auto is_done = GetNextOffsetChunk(context, gstate, lstate);
        if(is_done) {
            return;
        }
        rows_to_add = std::min((idx_t)STANDARD_VECTOR_SIZE, lstate.row_count - lstate.current_row);
    }

    idx_t page_size = 4096;
    idx_t margin = 256; // 256 bytes of margin to avoid page boundary issues.
    auto buffer = lstate.arena.Allocate(page_size);
    idx_t buffer_offset = 0;
    bool buffer_initialized = false;

    int64_t file_size = lstate.handle->GetFileSize();

    for(idx_t i = 0; i < rows_to_add; i++) {
        auto row_idx = lstate.current_row++;
        auto row_offset = lstate.GetOffsetForRow(row_idx);
        int64_t data_remaining = file_size - row_offset;
        
        if(row_offset == 0) {
            // Deleted row, just null everything
            for(idx_t j = 0; j < output.ColumnCount(); j++) {
                FlatVector::SetNull(output.data[j], i, true);
            }
        }

        // Is the offset in the buffer?
        if(row_offset < buffer_offset || row_offset + margin >= buffer_offset + page_size || !buffer_initialized) {
            // No, read the page into the buffer
            // But truncate if we try to read past the end of the file
            page_size = std::min((int64_t)4096, data_remaining);
            lstate.handle->Read(buffer, page_size, row_offset);
            buffer_offset = row_offset;
            buffer_initialized = true;
        }        
        // Where the row starts in the buffer
        auto row_buffer_offset = (row_offset - buffer_offset);

        // Read the row header
        int32_t length = Load<int32_t>(buffer + row_buffer_offset);
        // TODO: Read nullmask too before anything else!

        // Expand the buffer if needed
        if(row_buffer_offset + length > page_size) {
            auto new_page_size = std::max((int32_t)std::min((int64_t)4096, data_remaining), length);
            buffer = lstate.arena.Reallocate(buffer, page_size, new_page_size);
            page_size = new_page_size;

            // Move the file pointer back to the start of the page
            buffer_offset = row_offset;
            lstate.handle->Read(buffer, page_size, row_offset);
        }
        // TODO: read the row, now that we know it fits entirely in the buffer

        FlatVector::GetData<int64_t>(output.data[0])[i] = page_size;

        // just null everything for now
        for(idx_t j = 1; j < output.ColumnCount(); j++) {
            FlatVector::SetNull(output.data[j], i, true);
        }
    }
    output.SetCardinality(rows_to_add);
}

//------------------------------------------------------------------------------
// MISC
//------------------------------------------------------------------------------
static double Progress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) {
	auto &state = (GlobalState &)*global_state;
	return state.GetProgress();
}

static idx_t GetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                           LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &state = (LocalState &)*local_state;
	return state.batch_index;
}

static unique_ptr<TableRef> ReadGDBReplacementScan(ClientContext &context, const string &table_name, ReplacementScanData *data){
	// Check if the table name ends with .osm.pbf
	if(!StringUtil::EndsWith(StringUtil::Lower(table_name), ".gdbtable")) {
		return nullptr;
	}

	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("ST_ReadGDB", std::move(children));
	return std::move(table_function);
}

//------------------------------------------------------------------------------
//  REGISTER
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterReadGDB(ClientContext &context) {
	TableFunction read("ST_ReadGDB", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal, InitLocal);

	read.get_batch_index = GetBatchIndex;
	read.table_scan_progress = Progress;
	
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo info(read);
	catalog.CreateTableFunction(context, &info);

	// Replacement scan
	auto &config = DBConfig::GetConfig(*context.db);
	config.replacement_scans.emplace_back(ReadGDBReplacementScan);
}

} // namespace core

} // namespace spatial