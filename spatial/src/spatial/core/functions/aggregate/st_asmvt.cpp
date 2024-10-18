#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/bbox.hpp"
#include "spatial/core/functions/aggregate.hpp"
#include "spatial/core/types.hpp"

#include "spatial/core/geometry/geometry_processor.hpp"

#include "protozero/pbf_writer.hpp"

namespace spatial {

namespace core {

// Include protozero
namespace pz = protozero;

//------------------------------------------------------------------------
// Conversion
//------------------------------------------------------------------------

// This requires that the input geometry is already normalized to integer coordinates
// As well as all polygon rings have the correct winding order
class MVTSerializer final : GeometryProcessor<void, vector<uint32_t>&> {

	static constexpr uint32_t CMD_MOVE_TO = 1;
	static constexpr uint32_t CMD_LINE_TO = 2;
	static constexpr uint32_t CMD_CLOSE_PATH = 7;

	static uint32_t ZigZagEncode(uint32_t value) {
		return (value << 1) ^ (value >> 31);
	}

	static uint32_t AddCommand(uint32_t cmd, uint32_t count) {
		return (cmd & 0x7) | (count << 3);
	}

protected:
	static void ProcessVertices(const VertexData &vertices, vector<uint32_t> &commands, bool should_close) {
		auto count = vertices.count;
		idx_t i = 0;

		if(count == 0) {
			throw InvalidInputException("ST_AsMVT: does not support EMPTY geometries");
		}

		// Add a single MOVE_TO command
		commands.push_back(AddCommand(CMD_MOVE_TO, 1));
		{
			const auto x = Load<double>(vertices.data[0]);
			const auto y = Load<double>(vertices.data[1]);

			const auto fx = static_cast<float>(x);
			const auto fy = static_cast<float>(y);

			const auto ix = static_cast<uint32_t>(fx);
			const auto iy = static_cast<uint32_t>(fy);

			commands.push_back(ZigZagEncode(ix));
			commands.push_back(ZigZagEncode(iy));
		}

		i++;

		if(i == count) {
			return;
		}

		if(should_close) {
			count -= 1;
		}

		commands.push_back(AddCommand(CMD_LINE_TO, count - 1));
		for(; i < count; i++) {
			const auto x = Load<double>(vertices.data[0] + i * vertices.stride[0]);
			const auto y = Load<double>(vertices.data[1] + i * vertices.stride[1]);

			const auto fx = static_cast<float>(x);
			const auto fy = static_cast<float>(y);

			const auto ix = static_cast<uint32_t>(fx);
			const auto iy = static_cast<uint32_t>(fy);

			commands.push_back(ZigZagEncode(ix));
			commands.push_back(ZigZagEncode(iy));
		}

		// Add a CLOSE_PATH command
		if(should_close) {
			commands.push_back(AddCommand(CMD_CLOSE_PATH, 1));
		}
	}

	void ProcessPoint(const VertexData &vertices, vector<uint32_t> &commands) override {
		if(vertices.count == 0) {
			throw InvalidInputException("ST_AsMVT: does not support EMPTY points");
		}
		ProcessVertices(vertices, commands, false);
	}

	void ProcessLineString(const VertexData &vertices, vector<uint32_t> &commands) override {
		if(vertices.count == 0) {
			throw InvalidInputException("ST_AsMVT: does not support EMPTY linestrings");
		}
		ProcessVertices(vertices, commands, false);
	}

	void ProcessPolygon(PolygonState &state, vector<uint32_t> &commands) override {
		if(state.RingCount() == 0) {
			throw InvalidInputException("ST_AsMVT: does not support EMPTY polygons");
		}
		while(!state.IsDone()) {
			auto vertices = state.Next();
			ProcessVertices(vertices, commands, true);
		}
	}

	void ProcessCollection(CollectionState &state, vector<unsigned int> & commands) override {
		if(state.ItemCount() == 0) {
			throw InvalidInputException("ST_AsMVT: does not support EMPTY multi-geometries");
		}
		if(CurrentType() == GeometryType::GEOMETRYCOLLECTION) {
			throw InvalidInputException("ST_AsMVT does not support geometry collections");
		}

		while(!state.IsDone()) {
			state.Next(commands);
		}
	}
public:
	void Execute(const geometry_t &geometry, vector<uint32_t> &commands) {
		Process(geometry, commands);
	}
};


//------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------

struct AsMVTData final : FunctionData {
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq_base<FunctionData, AsMVTData>();
	}
	bool Equals(const FunctionData &other) const override {
		return other.Cast<AsMVTData>().geometry_column_idx == geometry_column_idx;
	}

	idx_t column_count = 0;
	idx_t geometry_column_idx = 0;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
															  vector<unique_ptr<Expression>> &arguments) {
	// First argument is the row struct
	const auto &row_struct_type = arguments[0]->return_type;
	if(row_struct_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("First argument to ST_AsMVT must be a row struct");
	}

	// Find the geometry column
	const auto &component_types = StructType::GetChildTypes(row_struct_type);
	optional_idx geometry_column_idx;
	for(idx_t i = 0; i < component_types.size(); i++) {
		if(component_types[i].second == GeoTypes::GEOMETRY()) {
			geometry_column_idx = i;
			break;
		}
	}
	if(!geometry_column_idx.IsValid()) {
		throw BinderException("ST_AsMVT requires a geometry column");
	}

	auto result = make_uniq<AsMVTData>();
	result->geometry_column_idx = geometry_column_idx.GetIndex();
	result->column_count = component_types.size();

	return std::move(result);
}

//------------------------------------------------------------------------
// State
//------------------------------------------------------------------------
struct MVTFeature {
	vector<uint32_t> geometry;
	int32_t geom_type = 0;
};

struct AsMVTAggState {
	bool is_set = false;
	vector<MVTFeature> features;
};

static idx_t StateSize(const AggregateFunction &function) {
	return sizeof(AsMVTAggState);
}

static void StateInitialize(const AggregateFunction &function, data_ptr_t state) {
	auto state_ptr = reinterpret_cast<AsMVTAggState *>(state);
	new(state_ptr) AsMVTAggState();
}

static void StateDestroy(Vector &state, AggregateInputData &aggr_input_data, idx_t count) {
	const auto state_ptr = reinterpret_cast<AsMVTAggState *>(state.GetData());
	state_ptr->~AsMVTAggState();
}

//------------------------------------------------------------------------
// Update
//------------------------------------------------------------------------

// argh, this is simple update
static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states, idx_t count) {
	throw NotImplementedException("ScatterUpdate not implemented for ST_AsMVT");
}

static void SimpleUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state_ptr, idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<AsMVTData>();
	auto &state = *reinterpret_cast<AsMVTAggState *>(state_ptr);

	// We only take one input, the rest are constant folded during binding
	auto &input = inputs[0];
	auto &cols = StructVector::GetEntries(input);
	for(idx_t col_idx = 0; col_idx < bind_data.column_count; col_idx++) {

		UnifiedVectorFormat format;
		cols[col_idx]->ToUnifiedFormat(count, format);

		if(col_idx == bind_data.geometry_column_idx) {
			// Geometry column!
			for(idx_t out_idx = 0; out_idx < count; out_idx++) {
				const auto in_idx = format.sel->get_index(out_idx);
				if(!format.validity.RowIsValid(in_idx)) {
					continue;
				}

				// Create a new feature
				state.is_set = true;
				state.features.emplace_back();
				auto &feature = state.features.back();

				// Get the geometry
				auto &geom = UnifiedVectorFormat::GetData<geometry_t>(format)[in_idx];

				// Set the type
				switch(geom.GetType()) {
					case GeometryType::POINT:
					case GeometryType::MULTIPOINT:
						feature.geom_type = 1;
					break;
					case GeometryType::LINESTRING:
					case GeometryType::MULTILINESTRING:
						feature.geom_type = 2;
					break;
					case GeometryType::POLYGON:
					case GeometryType::MULTIPOLYGON:
						feature.geom_type = 3;
						break;
				default:
					throw InvalidInputException("ST_AsMVT does not support geometry type: '%s'",
						GeometryTypes::ToString(geom.GetType()));
				}

				// Serialize the geometry to the mvt command format
				MVTSerializer serializer;
				serializer.Execute(geom, feature.geometry);
			}
		} else {
			// Attribute column!

		}
	}
}

//------------------------------------------------------------------------
// Combine
//------------------------------------------------------------------------
static void Combine(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	auto sdata = FlatVector::GetData<const AsMVTAggState*>(state);
	auto tdata = FlatVector::GetData<AsMVTAggState*>(combined);

	for(idx_t i = 0; i < count; i++) {
		auto &source = *sdata[i];
		auto &target = *tdata[i];

		if(!source.is_set) {
			continue;
		}
		if(!target.is_set) {
			target = source;
			continue;
		}

		// Combine the features
		target.features.insert(target.features.end(), source.features.begin(), source.features.end());
	}
}

//------------------------------------------------------------------------
// Finalize
//------------------------------------------------------------------------
static void FinalizeState(const AsMVTAggState &state, string_t& result, const AggregateFinalizeData &input, string &buffer) {
	// Reset the buffer
	buffer.clear();

	{
		// Create a new writer for the tile
		pz::pbf_writer tile_writer(buffer);

		// Create a new writer for the layer
		pz::pbf_writer layer_writer(tile_writer, 3);

		layer_writer.add_uint32(15, 1);
		layer_writer.add_string(1, "My Layer");


		for(idx_t i = 0; i < state.features.size(); i++) {
			const auto &feature = state.features[i];
			pz::pbf_writer feature_writer(layer_writer, 2);

			// ID
			feature_writer.add_uint64(1, i);
			vector<uint32_t> tags;
			feature_writer.add_packed_uint32(2, tags.begin(), tags.end());
			// Type
			feature_writer.add_enum(3, feature.geom_type);
			// Geometry
			feature_writer.add_packed_uint32(4, feature.geometry.begin(), feature.geometry.end());
		}

		// Extent
		layer_writer.add_uint32(5, 4096);
	}

	result = StringVector::AddStringOrBlob(input.result, buffer);
}

static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count, idx_t offset) {

	string buffer;

	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		const auto sdata = ConstantVector::GetData<AsMVTAggState *>(states);
		const auto rdata = ConstantVector::GetData<string_t>(result);

		AggregateFinalizeData finalize_data(result, aggr_input_data);
		FinalizeState(**sdata, *rdata, finalize_data, buffer);
	} else {
		D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		const auto sdata = FlatVector::GetData<AsMVTAggState *>(states);
		const auto rdata = FlatVector::GetData<string_t>(result);
		AggregateFinalizeData finalize_data(result, aggr_input_data);
		for (idx_t i = 0; i < count; i++) {
			finalize_data.result_idx = i + offset;
			FinalizeState(*sdata[i], rdata[finalize_data.result_idx], finalize_data, buffer);
		}
	}

}

//------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------
void CoreAggregateFunctions::RegisterStAsMVT(DatabaseInstance &db) {

	AggregateFunction function("ST_AsMVT",
		{LogicalType::ANY},
		LogicalType::BLOB,
		StateSize,
		StateInitialize,
		ScatterUpdate,
		Combine,
		Finalize,
		FunctionNullHandling::SPECIAL_HANDLING,
		SimpleUpdate,
		Bind,
		StateDestroy
	);

	/*
		const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
		aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
		aggregate_combine_t combine, aggregate_finalize_t finalize
	 */

	// Register the function
	ExtensionUtil::RegisterFunction(db, function);

	//DocUtil::AddDocumentation(db, "ST_Extent_Agg", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}


}

}