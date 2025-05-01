#include "geojson_module.hpp"

#include "spatial/spatial_types.hpp"
#include "spatial/util/function_builder.hpp"
#include "duckdb/main/extension_util.hpp"

// Extra
#include "yyjson.h"

namespace duckdb {

using namespace duckdb_yyjson_spatial;

namespace {

struct json {
	static const char* get_str_or_throw(yyjson_val* obj, const char* key) {
		const auto val = yyjson_obj_get(obj, key);
		if (!val) {
			throw InvalidInputException("Could not find key '%s' in GeoJSON object", key);
		}
		if (!yyjson_is_str(val)) {
			throw InvalidInputException("Key '%s' in GeoJSON object is not a string", key);
		}
		return yyjson_get_str(val);
	}

	static yyjson_val* get_obj_or_throw(yyjson_val* obj, const char* key) {
		const auto val = yyjson_obj_get(obj, key);
		if (!val) {
			throw InvalidInputException("Could not find key '%s' in GeoJSON object", key);
		}
		if (!yyjson_is_obj(val)) {
			throw InvalidInputException("Key '%s' in GeoJSON object is not an object", key);
		}
		return val;
	}

	static yyjson_val* get_arr_or_throw(yyjson_val* obj, const char* key) {
		const auto val = yyjson_obj_get(obj, key);
		if (!val) {
			throw InvalidInputException("Could not find key '%s' in GeoJSON object", key);
		}
		if (!yyjson_is_arr(val)) {
			throw InvalidInputException("Key '%s' in GeoJSON object is not an array", key);
		}
		return val;
	}
};



//======================================================================================================================
// GeoJSON Allocator
//======================================================================================================================

class JSONAllocator {
	// Stolen from the JSON extension :)
public:
	explicit JSONAllocator(ArenaAllocator &allocator)
		: allocator(allocator), yyjson_allocator({Allocate, Reallocate, Free, &allocator}) {
	}
	yyjson_alc *GetYYJSONAllocator() {
		return &yyjson_allocator;
	}
	void Reset() {
		allocator.Reset();
	}

private:
	static void *Allocate(void *ctx, size_t size) {
		const auto alloc = static_cast<ArenaAllocator *>(ctx);
		return alloc->AllocateAligned(size);
	}
	static void *Reallocate(void *ctx, void *ptr, size_t old_size, size_t size) {
		const auto alloc = static_cast<ArenaAllocator *>(ctx);
		return alloc->ReallocateAligned(data_ptr_cast(ptr), old_size, size);
	}
	static void Free(void *ctx, void *ptr) {
		// NOP because ArenaAllocator can't free
	}
	ArenaAllocator &allocator;
	yyjson_alc yyjson_allocator;
};



//======================================================================================================================
// GeoJSON Scan
//======================================================================================================================

class JSONObjectScanner {

	enum class State {
		START,
		IN_OBJECT,
		IN_STRING,
	};

	State state = State::START;
	idx_t nesting = 0;

	char* pos = nullptr;
	char* end = nullptr;
	char buffer[1024] = {};

	vector<char> object;
public:
	JSONObjectScanner() = default;

	vector<char> &GetObject() { return object; }

	// Returns true if we found a complete object, false if the file is exhausted
	bool TryScanNextObject(unique_ptr<FileHandle> &handle) {
		state = State::START;
		object.clear();

		while(true) {
			if(pos >= end) {
				const auto read_size = handle->Read(buffer, sizeof(buffer));
				if(read_size == 0) {
					// EOF, Maybe return NEED_MORE_INPUT here?
					return false;
				}
				pos = buffer;
				end = buffer + read_size;
			}

			char* obj_beg = pos;
			char* obj_end = end;

			while(pos < obj_end) {
				switch (state) {
				case State::START:
					switch (*pos) {
						case '{':
							state = State::IN_OBJECT;
							nesting++;
							obj_beg = pos;
							pos++;
						break;
						case ' ':
						case '\n':
						case '\r':
						case '\t':
							pos++;
						break;
						default:
							throw InvalidInputException("Invalid GeoJSON file: expected '{' at the beginning");
					}
				break;
				case State::IN_OBJECT:
					switch (*pos) {
						case '{':
							nesting++;
							pos++;
						break;
						case '}':
							nesting--;
							pos++;
							if(nesting == 0) {
								obj_end = pos;
								state = State::START;
								// We found a complete object, return it
								object.insert(object.end(), obj_beg, obj_end);
								return true;
							}
						break;
						case '"':
							pos++;
							state = State::IN_STRING;
						break;
						default:
							pos++;
						break;
					}
					break;
				case State::IN_STRING:
					switch (*pos) {
						case '"':
							pos++;
							state = State::IN_OBJECT;
						break;
						case '\\':
							pos++;
						default:
							pos++;
						break;
					}
				break;
				default:
					throw InvalidInputException("Invalid GeoJSON file: unexpected state");
				}
			}

			// Append the bytes we read to the output
			object.insert(object.end(), obj_beg, obj_end);
		}
	}
};


struct ST_ReadGeoJSON {

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------
	class ReadGeoJSONData final : public TableFunctionData {
	public:
		string file_path;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_uniq<ReadGeoJSONData>();

		return_types.push_back(GeoTypes::GEOMETRY());
		names.push_back("geometry");

		// No schema inference for now
		return_types.push_back(LogicalType::JSON());
		names.push_back("properties");

		result->file_path = input.inputs[0].GetValue<string>();

		return std::move(result);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------
	class ReadGeoJSONState final : public GlobalTableFunctionState {
	public:
		explicit ReadGeoJSONState(ClientContext &context) : arena(BufferAllocator::Get(context)),
			allocator(arena) {
		}

		unique_ptr<FileHandle> file_handle;
		ArenaAllocator arena;
		JSONAllocator allocator;
		JSONObjectScanner scanner;

		bool geojson_is_collection = false;
		yyjson_doc* geojson_doc = nullptr;
		yyjson_arr_iter feature_iter = {};
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bdata = input.bind_data->Cast<ReadGeoJSONData>();
		auto result = make_uniq<ReadGeoJSONState>(context);

		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(bdata.file_path, FileFlags::FILE_FLAGS_READ);

		result->file_handle = std::move(file_handle);

		return std::move(result);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------
	static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bdata = data_p.bind_data->Cast<ReadGeoJSONData>();
		auto &gstate = data_p.global_state->Cast<ReadGeoJSONState>();

		auto &geom_vec = output.data[0];
		auto &prop_vec = output.data[1];

		auto &file_handle = gstate.file_handle;

		idx_t output_idx = 0;

		while(output_idx < output.GetCapacity()) {

			if(!gstate.geojson_doc) {
				if(!gstate.scanner.TryScanNextObject(file_handle)) {
					// Out of input!
					break;
				}

				// Reset allocator...?
				gstate.allocator.Reset();

				// Need a new object
				auto &object = gstate.scanner.GetObject();
				yyjson_read_err err;
				gstate.geojson_doc = yyjson_read_opts(object.data(), object.size(),
											YYJSON_READ_ALLOW_TRAILING_COMMAS | YYJSON_READ_ALLOW_COMMENTS,
											gstate.allocator.GetYYJSONAllocator(), &err);

				const auto root = yyjson_doc_get_root(gstate.geojson_doc);
				if (!yyjson_is_obj(root)) {
					throw InvalidInputException("Could not parse GeoJSON input: %s", err.msg);
				}
				const auto type = json::get_str_or_throw(root, "type");
				if(strcmp(type, "Feature") == 0) {
					gstate.geojson_is_collection = false;
				} else if (strcmp(type, "FeatureCollection") == 0) {
					const auto features = json::get_arr_or_throw(root, "features");
					gstate.geojson_is_collection = true;
					yyjson_arr_iter_init(features, &gstate.feature_iter);
				} else {
					throw InvalidInputException("Invalid GeoJSON input: expected 'Feature' or 'FeatureCollection'");
				}
			}

			// Now, we have a feature, or a feature collection
			yyjson_val* feature = nullptr;
			if(gstate.geojson_doc) {
				feature = yyjson_arr_iter_next(&gstate.feature_iter);
				if (!feature) {
					// No more features in the collection
					gstate.geojson_doc = nullptr;
					continue;
				}
			} else {
				feature = yyjson_doc_get_root(gstate.geojson_doc);
				gstate.geojson_doc = nullptr;
			}

			const auto properties = json::get_obj_or_throw(feature, "properties");
			const auto geometry = json::get_obj_or_throw(feature, "geometry");

			size_t len;
			yyjson_write_err err;
			const auto properties_str = yyjson_val_write_opts(properties, 0, gstate.allocator.GetYYJSONAllocator(), &len, &err);
			FlatVector::SetNull(geom_vec, output_idx, true);
			FlatVector::GetData<string_t>(prop_vec)[output_idx] = StringVector::AddString(prop_vec, properties_str, len);

			// Increment output index
			output_idx++;
		}

		output.SetCardinality(output_idx);
	}


	//------------------------------------------------------------------------------------------------------------------
	// DOCUMENTATION
	//------------------------------------------------------------------------------------------------------------------
	static constexpr auto DESCRIPTION = "";
	static constexpr auto EXAMPLE = "";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------
	static void Register(DatabaseInstance &db) {
		const TableFunction func("ST_ReadGeoJSON", {LogicalType::VARCHAR}, Execute, Bind, Init);

		ExtensionUtil::RegisterFunction(db, func);
		FunctionBuilder::AddTableFunctionDocs(db, "ST_GeneratePoints", DESCRIPTION, EXAMPLE, {{"ext", "spatial"}});
	}
};

} // namespace

//######################################################################################################################
// Register
//######################################################################################################################

void RegisterGeoJSONModule(DatabaseInstance &db) {
	ST_ReadGeoJSON::Register(db);
}

} // namespace duckdb