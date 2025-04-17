#include "spatial/modules/fgb/fgb_module.hpp"

#include "spatial/modules/fgb/generated/feature_generated.hpp"
#include "spatial/modules/fgb/generated/header_generated.hpp"

#include <duckdb/main/database.hpp>
#include <duckdb/main/extension_util.hpp>
#include <sgl/sgl.hpp>
#include <spatial/geometry/geometry_serialization.hpp>
#include <spatial/spatial_types.hpp>
#include <spatial/util/binary_reader.hpp>

namespace duckdb {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
class FGBBindData final : public TableFunctionData {
public:
	string file_path;
	uint64_t feature_count = 0;

	uint64_t header_byte_size = 0;
	uint64_t index_byte_size = 0;

	uint16_t index_node_size = 0;
	uint64_t index_nodes_count = 0;
};

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
									 vector<LogicalType> &return_types, vector<string> &names) {
	auto bdata = make_uniq<FGBBindData>();


	const auto file_path = input.inputs[0].GetValue<string>();
	if (file_path.empty()) {
		throw InvalidInputException("File path cannot be empty");
	}
	bdata->file_path = file_path;


	auto &fs = FileSystem::GetFileSystem(context);
	const auto file = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);

	char sig[8];
	if(file->Read(sig, 8) != 8) {
		throw InvalidInputException("Failed to read file header");
	}
	if (sig[0] != 'f' || sig[1] != 'g' || sig[2] != 'b' || sig[4] != 'f' || sig[5] != 'g' || sig[6] != 'b') {
		throw InvalidInputException("Invalid file format");
	}
	auto major_version = sig[3];
	auto minor_version = sig[7];

	// Now, read the header
	uint32_t size = 0;
	if (file->Read((char *)&size, sizeof(uint32_t)) != sizeof(uint32_t)) {
		throw InvalidInputException("Failed to read file header");
	}

	bdata->header_byte_size = size;

	// Read the header
	const auto header_buf = make_uniq_array_uninitialized<uint8_t>(size);
	if (file->Read((char *)header_buf.get(), size) != size) {
		throw InvalidInputException("Failed to read file header");
	}

	const auto header = FlatGeobuf::GetHeader(header_buf.get());

	// Get the feature count
	bdata->feature_count = header->features_count();

	// If the node size is 0, then there is no index
	bdata->index_node_size = header->index_node_size();
	if(bdata->index_node_size != 0) {

		// Compute the total number of nodes in the index
		const auto node_size = header->index_node_size();

		auto level_count = bdata->feature_count;
		auto nodes_count = level_count;

		do {
			level_count = (level_count + node_size - 1) / node_size;
			nodes_count += level_count;
		} while (level_count > 1);

		bdata->index_nodes_count = nodes_count;
		bdata->index_byte_size = nodes_count * 40;
	}

	// Add the geometry type
	names.push_back("geom");
	return_types.push_back(GeoTypes::GEOMETRY());

	// Convert types of the attributes
	const auto columns = header->columns();
	for (const auto &col : *columns) {
		names.push_back(col->name()->str());
		switch (col->type()) {
		case FlatGeobuf::ColumnType::Byte:
			return_types.push_back(LogicalType::TINYINT);
			break;
		case FlatGeobuf::ColumnType::UByte:
			return_types.push_back(LogicalType::UTINYINT);
			break;
		case FlatGeobuf::ColumnType::Bool:
			return_types.push_back(LogicalType::BOOLEAN);
			break;
		case FlatGeobuf::ColumnType::Short:
			return_types.push_back(LogicalType::SMALLINT);
			break;
		case FlatGeobuf::ColumnType::UShort:
			return_types.push_back(LogicalType::USMALLINT);
			break;
		case FlatGeobuf::ColumnType::Int:
			return_types.push_back(LogicalType::INTEGER);
			break;
		case FlatGeobuf::ColumnType::UInt:
			return_types.push_back(LogicalType::UINTEGER);
			break;
		case FlatGeobuf::ColumnType::Long:
			return_types.push_back(LogicalType::BIGINT);
			break;
		case FlatGeobuf::ColumnType::ULong:
			return_types.push_back(LogicalType::UBIGINT);
			break;
		case FlatGeobuf::ColumnType::Float:
			return_types.push_back(LogicalType::FLOAT);
			break;
		case FlatGeobuf::ColumnType::Double:
			return_types.push_back(LogicalType::DOUBLE);
			break;
		case FlatGeobuf::ColumnType::String:
			return_types.push_back(LogicalType::VARCHAR);
			break;
		case FlatGeobuf::ColumnType::Json:
			return_types.push_back(LogicalType::JSON());
			break;
		case FlatGeobuf::ColumnType::DateTime:
			return_types.push_back(LogicalType::TIMESTAMP);
			break;
		case FlatGeobuf::ColumnType::Binary:
			return_types.push_back(LogicalType::BLOB);
			break;
		default:
			throw InvalidInputException("Unsupported column type");
		}
	}

	// Rename columns if they are duplicates
	QueryResult::DeduplicateColumns(names);

	return std::move(bdata);
}


//----------------------------------------------------------------------------------------------------------------------
// Global State
//----------------------------------------------------------------------------------------------------------------------
class FGBGlobalState final : public GlobalTableFunctionState {
public:
	explicit FGBGlobalState(ClientContext &context)
		: arena(BufferAllocator::Get(context)), file(nullptr) { }

	ArenaAllocator arena;
	vector<const FlatGeobuf::Feature *> features;
	unique_ptr<FileHandle> file;
	idx_t current_feature_idx = 0;
};

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<FGBBindData>();
	auto gstate = make_uniq<FGBGlobalState>(context);

	// Open file again
	auto &fs = FileSystem::GetFileSystem(context);
	auto file = fs.OpenFile(bdata.file_path, FileFlags::FILE_FLAGS_READ);

	// Seek to the data section
	// Add +8 here to skip the signature, +4 for the header size
	file->Seek(file->SeekPosition() + 12 + bdata.header_byte_size + bdata.index_byte_size);

	// Pass the file handle to the global state
	gstate->file = std::move(file);

	return std::move(gstate);
}


//----------------------------------------------------------------------------------------------------------------------
// Local State
//----------------------------------------------------------------------------------------------------------------------
/*
class FGBLocalState final : public LocalTableFunctionState {
public:

};

unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
													 GlobalTableFunctionState *global_state) {
	auto lstate = make_uniq<FGBLocalState>();

	return std::move(lstate);
}
*/

//----------------------------------------------------------------------------------------------------------------------
// Execute
//----------------------------------------------------------------------------------------------------------------------
void DecodeGeometry(ArenaAllocator &arena, const FlatGeobuf::Geometry *input, sgl::geometry *geom) {

	// First, initialize based on type
	switch (input->type()) {
	case FlatGeobuf::GeometryType::Point: sgl::point::init_empty(geom); break;
	case FlatGeobuf::GeometryType::LineString: sgl::linestring::init_empty(geom); break;
	case FlatGeobuf::GeometryType::Polygon: sgl::polygon::init_empty(geom); break;
	case FlatGeobuf::GeometryType::MultiPoint: sgl::multi_point::init_empty(geom); break;
	case FlatGeobuf::GeometryType::MultiLineString: sgl::multi_linestring::init_empty(geom); break;
	case FlatGeobuf::GeometryType::MultiPolygon: sgl::multi_polygon::init_empty(geom); break;
	case FlatGeobuf::GeometryType::GeometryCollection: sgl::multi_geometry::init_empty(geom); break;
	default: {
		const auto name = FlatGeobuf::EnumNameGeometryType(input->type());
		throw NotImplementedException("Unsupported FlatGeoBuf geometry type: '%s'", name);
	}
	}

	// Second, decode the geometry
	switch (geom->get_type()) {
		case sgl::geometry_type::POINT:
		case sgl::geometry_type::LINESTRING: {
			const auto vertex_array = input->xy()->data();
			const auto vertex_count = input->xy()->size() / 2;
			geom->set_vertex_data(vertex_array, vertex_count);
		} break;
		case sgl::geometry_type::POLYGON:
		case sgl::geometry_type::MULTI_LINESTRING: {
			if(!input->ends() || input->ends()->empty()) {
				// Single part, no ends
				const auto part_mem = arena.AllocateAligned(sizeof(sgl::geometry));
				const auto part_ptr = new (part_mem) sgl::geometry();

				sgl::linestring::init_empty(part_ptr);

				const auto vertex_array = input->xy()->data();
				const auto vertex_count = input->xy()->size() / 2;

				part_ptr->set_vertex_data(vertex_array, vertex_count);
				geom->append_part(part_ptr);
			} else {
				uint32_t beg = 0;
				for(const uint32_t end : *input->ends()) {
					const auto part_mem = arena.AllocateAligned(sizeof(sgl::geometry));
					const auto part_ptr = new (part_mem) sgl::geometry();

					sgl::linestring::init_empty(part_ptr);

					const auto vertex_array = input->xy()->data() + beg * 2;
					const auto vertex_count = (end - beg);

					part_ptr->set_vertex_data(vertex_array, vertex_count);
					geom->append_part(part_ptr);
					beg = end;
				}
			}
		} break;
		case sgl::geometry_type::MULTI_POINT: {
			sgl::multi_point::init_empty(geom);

			const auto vertex_array = input->xy()->data();
			const auto vertex_count = input->xy()->size() / 2;

			for(uint32_t i = 0; i < vertex_count; i++) {
				const auto point_mem = arena.AllocateAligned(sizeof(sgl::geometry));
				const auto point_ptr = new (point_mem) sgl::geometry();

				sgl::point::init_empty(point_ptr);
				point_ptr->set_vertex_data(vertex_array + i * 2, 1);
				geom->append_part(point_ptr);
			}
		} break;
		case sgl::geometry_type::MULTI_POLYGON:
		case sgl::geometry_type::MULTI_GEOMETRY: {
			for(const auto &part : *input->parts()) {
				const auto part_mem = arena.AllocateAligned(sizeof(sgl::geometry));
				const auto part_ptr = new (part_mem) sgl::geometry();

				// TODO: Make non-recursive
				DecodeGeometry(arena, part, part_ptr);

				geom->append_part(part_ptr);
			}
		} break;
		default:
			throw InvalidInputException("Unsupported geometry type");
	}
}

void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bdata = input.bind_data->Cast<FGBBindData>();
	auto &gstate = input.global_state->Cast<FGBGlobalState>();

	// Reset arena
	gstate.arena.Reset();
	gstate.features.clear();

	// First pass, collect as many features as we can
	const auto batch_size = MinValue(output.GetCapacity(), bdata.feature_count - gstate.current_feature_idx);
	for(idx_t row_idx = 0; row_idx < batch_size; row_idx++) {
		uint32_t size = 0;
		if (gstate.file->Read((char *)&size, sizeof(uint32_t)) != sizeof(uint32_t)) {
			throw InvalidInputException("Failed to read feature");
		}

		const auto ptr = gstate.arena.AllocateAligned(size);
		if (gstate.file->Read((char *)ptr, size) != size) {
			throw InvalidInputException("Failed to read feature");
		}

		auto feature = FlatGeobuf::GetFeature(ptr);

		gstate.features.push_back(feature);
	}

	// Second pass, fill the output by parsing the features
	auto &geom_vec = output.data[0];
	auto geom_ptr = FlatVector::GetData<string_t>(geom_vec);

	for(idx_t row_idx = 0; row_idx < batch_size; row_idx++) {
		const auto &feature = gstate.features[row_idx];
		sgl::geometry geom;
		DecodeGeometry(gstate.arena, feature->geometry(), &geom);
		if(geom.get_type() == sgl::geometry_type::INVALID) {
			throw InvalidInputException("Invalid geometry");
		}

		const auto blob_len = Serde::GetRequiredSize(geom);
		auto blob = StringVector::EmptyString(geom_vec, blob_len);
		const auto blob_ptr = blob.GetDataWriteable();
		Serde::Serialize(geom, blob_ptr, blob_len);
		blob.Finalize();

		geom_ptr[row_idx] = blob;
	}

	// Now fill the rest of the columns
	for(idx_t row_idx = 0; row_idx < batch_size; row_idx++) {
		// TODO: Handle missing props (nulls)
		const auto &props = gstate.features[row_idx]->properties();
		BinaryReader reader(reinterpret_cast<const char*>(props->data()), props->size());
		while(!reader.IsDone()) {
			const auto key = reader.Read<uint16_t>();
			auto &col = output.data[key + 1];

			switch (col.GetType().id()) {
				case LogicalTypeId::BOOLEAN: {
					const auto value = reader.Read<uint8_t>();
					FlatVector::GetData<bool>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::TINYINT: {
					const auto value = reader.Read<int8_t>();
					FlatVector::GetData<int8_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::UTINYINT: {
					const auto value = reader.Read<uint8_t>();
					FlatVector::GetData<uint8_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::SMALLINT: {
					const auto value = reader.Read<int16_t>();
					FlatVector::GetData<int16_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::USMALLINT: {
					const auto value = reader.Read<uint16_t>();
					FlatVector::GetData<uint16_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::INTEGER: {
					const auto value = reader.Read<int32_t>();
					FlatVector::GetData<int32_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::UINTEGER: {
					const auto value = reader.Read<uint32_t>();
					FlatVector::GetData<uint32_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::BIGINT: {
					const auto value = reader.Read<int64_t>();
					FlatVector::GetData<int64_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::UBIGINT: {
					const auto value = reader.Read<uint64_t>();
					FlatVector::GetData<uint64_t>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::FLOAT: {
					const auto value = reader.Read<float>();
					FlatVector::GetData<float>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::DOUBLE: {
					const auto value = reader.Read<double>();
					FlatVector::GetData<double>(col)[row_idx] = value;
				} break;
				case LogicalTypeId::VARCHAR: {
					const auto size = reader.Read<uint32_t>();
					const auto str = reader.Reserve(size);
					FlatVector::GetData<string_t>(col)[row_idx] = string_t(str, size);
				} break;
				case LogicalTypeId::TIMESTAMP: {
					// TODO: What?
					const auto size = reader.Read<uint32_t>();
					const auto str = reader.Reserve(size);
					FlatVector::GetData<string_t>(col)[row_idx] = string_t(str, size);
				} break;
				case LogicalTypeId::BLOB: {
					const auto size = reader.Read<uint32_t>();
					const auto str = reader.Reserve(size);
					FlatVector::GetData<string_t>(col)[row_idx] = string_t(str, size);
				} break;
				default:
					throw NotImplementedException("Unsupported column type");
			}
		}
	}

	gstate.current_feature_idx += batch_size;

	output.SetCardinality(batch_size);
}

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------

} // namespace

void RegisterReadFGB(DatabaseInstance &db) {
	TableFunction read("read_fgb", {LogicalType::VARCHAR},
		Execute, Bind, InitGlobal/*,InitLocal*/);

	ExtensionUtil::RegisterFunction(db, read);

}

} // namespace duckdb