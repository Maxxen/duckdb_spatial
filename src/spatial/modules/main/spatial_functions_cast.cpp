#include "spatial/modules/main/spatial_functions.hpp"
#include "spatial/geometry/geometry_processor.hpp"
#include "spatial/geometry/sgl.hpp"
#include "spatial/geometry/geometry_serialization.hpp"
#include "spatial/spatial_types.hpp"
#include "spatial/util/math.hpp"
#include "spatial/geometry/wkb_writer.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

namespace {

//######################################################################################################################
// Util
//######################################################################################################################

//======================================================================================================================
// Local State
//======================================================================================================================

class LocalState final : public FunctionLocalState {
public:
	explicit LocalState(ClientContext &context) : arena(BufferAllocator::Get(context)), allocator(arena) {
	}

	static unique_ptr<FunctionLocalState> InitCast(CastLocalStateParameters &params);
	static LocalState &ResetAndGet(CastParameters &params);

	// De/Serialize geometries
	void Deserialize(const string_t &blob, sgl::geometry &geom);
	string_t Serialize(Vector &vector, const sgl::geometry &geom);

	ArenaAllocator &GetArena() {
		return arena;
	}
	GeometryAllocator &GetAllocator() {
		return allocator;
	}

private:
	ArenaAllocator arena;
	GeometryAllocator allocator;
};

unique_ptr<FunctionLocalState> LocalState::InitCast(CastLocalStateParameters &parameters) {
	return make_uniq<LocalState>(*parameters.context);
}

LocalState &LocalState::ResetAndGet(CastParameters &state) {
	auto &local_state = state.local_state->Cast<LocalState>();
	local_state.arena.Reset();
	return local_state;
}

void LocalState::Deserialize(const string_t &blob, sgl::geometry &geom) {
	Serde::Deserialize(geom, arena, blob.GetDataUnsafe(), blob.GetSize());
}

string_t LocalState::Serialize(Vector &vector, const sgl::geometry &geom) {
	const auto size = Serde::GetRequiredSize(geom);
	auto blob = StringVector::EmptyString(vector, size);
	Serde::Serialize(geom, blob.GetDataWriteable(), size);
	blob.Finalize();
	return blob;
}

//######################################################################################################################
// Cast Functions
//######################################################################################################################

//======================================================================================================================
// GEOMETRY Casts
//======================================================================================================================

struct GeometryCasts {

	//------------------------------------------------------------------------------------------------------------------
	// GEOMETRY -> VARCHAR
	//------------------------------------------------------------------------------------------------------------------
	static bool ToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &) {
		CoreVectorOperations::GeometryToVarchar(source, result, count);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// VARCHAR -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool FromVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);
		auto &alloc = lstate.GetAllocator();

		sgl::ops::wkt_reader reader = {};
		reader.alloc = &alloc;

		auto success = true;

		UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		    source, result, count, [&](const string_t &wkt, ValidityMask &mask, idx_t row_idx) {
			    const auto wkt_ptr = wkt.GetDataUnsafe();
			    const auto wkt_len = wkt.GetSize();

			    reader.buf = wkt_ptr;
			    reader.end = wkt_ptr + wkt_len;

			    sgl::geometry geom;

			    if (!sgl::ops::wkt_reader_try_parse(&reader, &geom)) {
				    if (success) {
					    success = false;
					    const auto error = sgl::ops::wkt_reader_get_error_message(&reader);
					    HandleCastError::AssignError(error, parameters.error_message);
				    }
				    mask.SetInvalid(row_idx);
				    return string_t {};
			    }

			    return lstate.Serialize(result, geom);
		    });

		return success;
	}

	//------------------------------------------------------------------------------------------------------------------
	// GEOMETRY -> WKB_BLOB
	//------------------------------------------------------------------------------------------------------------------
	static bool ToWKBCast(Vector &source, Vector &result, idx_t count, CastParameters &) {
		UnaryExecutor::Execute<string_t, string_t>(
		    source, result, count, [&](const string_t &input) { return WKBWriter::Write(input, result); });
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// WKB_BLOB -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool FromWKBCast(Vector &source, Vector &result, idx_t count, CastParameters &params) {
		auto &lstate = LocalState::ResetAndGet(params);
		auto &alloc = lstate.GetAllocator();

		constexpr auto MAX_STACK_DEPTH = 128;
		uint32_t recursion_stack[MAX_STACK_DEPTH];

		sgl::ops::wkb_reader reader = {};
		reader.copy_vertices = false;
		reader.alloc = &alloc;
		reader.allow_mixed_zm = false;
		reader.nan_as_empty = true;

		reader.stack_buf = recursion_stack;
		reader.stack_cap = MAX_STACK_DEPTH;

		bool success = true;

		UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
		    source, result, count, [&](const string_t &wkb, ValidityMask &mask, idx_t row_idx) {
			    reader.buf = wkb.GetDataUnsafe();
			    reader.end = reader.buf + wkb.GetSize();

			    sgl::geometry geom(sgl::geometry_type::INVALID);

			    // Try parse, if it fails, assign error message and return NULL
			    if (!sgl::ops::wkb_reader_try_parse(&reader, &geom)) {
				    const auto error = sgl::ops::wkb_reader_get_error_message(&reader);
				    if (success) {
					    success = false;
					    HandleCastError::AssignError(error, params.error_message);
				    }
				    mask.SetInvalid(row_idx);
				    return string_t {};
			    }

			    return lstate.Serialize(result, geom);
		    });

		return success;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------
	static void Register(DatabaseInstance &db) {
		const auto wkb_type = GeoTypes::WKB_BLOB();
		const auto geom_type = GeoTypes::GEOMETRY();

		// VARCHAR -> Geometry is explicitly castable
		ExtensionUtil::RegisterCastFunction(db, geom_type, LogicalType::VARCHAR, BoundCastInfo(ToVarcharCast), 1);

		// Geometry -> VARCHAR is implicitly castable
		ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, geom_type,
		                                    BoundCastInfo(FromVarcharCast, nullptr, LocalState::InitCast));

		// Geometry -> WKB is explicitly castable
		ExtensionUtil::RegisterCastFunction(db, geom_type, wkb_type, BoundCastInfo(ToWKBCast));

		// Geometry -> BLOB is explicitly castable
		ExtensionUtil::RegisterCastFunction(db, geom_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast);

		// WKB -> Geometry is explicitly castable
		ExtensionUtil::RegisterCastFunction(db, wkb_type, geom_type,
		                                    BoundCastInfo(FromWKBCast, nullptr, LocalState::InitCast));

		// WKB -> BLOB is implicitly castable
		ExtensionUtil::RegisterCastFunction(db, wkb_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
	}
};

//======================================================================================================================
// POINT_2D Casts
//======================================================================================================================

struct PointCasts {

	//------------------------------------------------------------------------------------------------------------------
	// POINT_2D -> VARCHAR
	//------------------------------------------------------------------------------------------------------------------
	static bool ToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		CoreVectorOperations::Point2DToVarchar(source, result, count);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// POINT_2D -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool ToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		using POINT_TYPE = StructTypeBinary<double, double>;
		using GEOMETRY_TYPE = PrimitiveType<string_t>;

		auto &lstate = LocalState::ResetAndGet(parameters);

		GenericExecutor::ExecuteUnary<POINT_TYPE, GEOMETRY_TYPE>(source, result, count, [&](const POINT_TYPE &point) {
			const double buffer[2] = {point.a_val, point.b_val};
			sgl::geometry geom(sgl::geometry_type::POINT, false, false);
			geom.set_vertex_data(reinterpret_cast<const uint8_t *>(buffer), 1);

			return lstate.Serialize(result, geom);
		});
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// GEOMETRY -> POINT_2D
	//------------------------------------------------------------------------------------------------------------------
	static bool FromGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		using POINT_TYPE = StructTypeBinary<double, double>;
		using GEOMETRY_TYPE = PrimitiveType<string_t>;

		auto &lstate = LocalState::ResetAndGet(parameters);

		GenericExecutor::ExecuteUnary<GEOMETRY_TYPE, POINT_TYPE>(source, result, count, [&](const GEOMETRY_TYPE &blob) {
			sgl::geometry geom;
			lstate.Deserialize(blob.val, geom);

			if (geom.get_type() != sgl::geometry_type::POINT) {
				throw ConversionException("Cannot cast non-point GEOMETRY to POINT_2D");
			}
			if (geom.is_empty()) {
				// TODO: Maybe make this return NULL instead
				throw ConversionException("Cannot cast empty point GEOMETRY to POINT_2D");
			}
			const auto vertex = geom.get_vertex_xy(0);
			return POINT_TYPE {vertex.x, vertex.y};
		});

		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// POINT(N) -> POINT_2D
	//------------------------------------------------------------------------------------------------------------------
	static bool ToPoint2DCast(Vector &source, Vector &result, idx_t count, CastParameters &) {
		auto &children = StructVector::GetEntries(source);
		const auto &x_child = children[0];
		const auto &y_child = children[1];

		const auto &result_children = StructVector::GetEntries(result);
		const auto &result_x_child = result_children[0];
		const auto &result_y_child = result_children[1];

		result_x_child->Reference(*x_child);
		result_y_child->Reference(*y_child);

		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------
	static void Register(DatabaseInstance &db) {
		// POINT_2D -> VARCHAR
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::POINT_2D(), LogicalType::VARCHAR,
		                                    BoundCastInfo(ToVarcharCast), 1);
		// POINT_2D -> GEOMETRY
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::POINT_2D(), GeoTypes::GEOMETRY(),
		                                    BoundCastInfo(ToGeometryCast, nullptr, LocalState::InitCast), 1);
		// GEOMETRY -> POINT_2D
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::GEOMETRY(), GeoTypes::POINT_2D(),
		                                    BoundCastInfo(FromGeometryCast, nullptr, LocalState::InitCast), 1);
		// POINT_3D -> POINT_2D
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::POINT_3D(), GeoTypes::POINT_2D(), ToPoint2DCast, 1);
		// POINT_4D -> POINT_2D
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::POINT_4D(), GeoTypes::POINT_2D(), ToPoint2DCast, 1);
	}
};

//======================================================================================================================
// LINESTRING_2D Casts
//======================================================================================================================

struct LinestringCasts {

	//------------------------------------------------------------------------------------------------------------------
	// LINESTRING_2D -> VARCHAR
	//------------------------------------------------------------------------------------------------------------------
	static bool ToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &) {
		CoreVectorOperations::LineString2DToVarchar(source, result, count);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// LINESTRING_2D -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool ToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);
		auto &arena = lstate.GetArena();

		auto &coord_vec = ListVector::GetEntry(source);
		auto &coord_vec_children = StructVector::GetEntries(coord_vec);
		const auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
		const auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

		UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](const list_entry_t &line) {
			const auto vertex_data_mem = arena.AllocateAligned(sizeof(double) * 2 * line.length);
			const auto vertex_data_ptr = reinterpret_cast<double *>(vertex_data_mem);

			for (idx_t i = 0; i < line.length; i++) {
				vertex_data_ptr[i * 2] = x_data[line.offset + i];
				vertex_data_ptr[i * 2 + 1] = y_data[line.offset + i];
			}

			sgl::geometry geom(sgl::geometry_type::LINESTRING, false, false);
			geom.set_vertex_data(vertex_data_mem, line.length);

			return lstate.Serialize(result, geom);
		});
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// GEOMETRY -> LINESTRING_2D
	//------------------------------------------------------------------------------------------------------------------
	static bool FromGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);

		auto &coord_vec = ListVector::GetEntry(result);
		auto &coord_vec_children = StructVector::GetEntries(coord_vec);
		const auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
		const auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

		idx_t total_coords = 0;

		UnaryExecutor::Execute<string_t, list_entry_t>(source, result, count, [&](const string_t &blob) {
			sgl::geometry line;
			lstate.Deserialize(blob, line);

			if (line.get_type() != sgl::geometry_type::LINESTRING) {
				// TODO: Dont throw here, return NULL instead to allow TRY_CAST
				throw ConversionException("Cannot cast non-linestring GEOMETRY to LINESTRING_2D");
			}

			const auto line_size = line.get_count();

			const auto entry = list_entry_t(total_coords, line_size);
			total_coords += line_size;
			ListVector::Reserve(result, total_coords);

			for (idx_t i = 0; i < line_size; i++) {
				const auto vertex = line.get_vertex_xy(i);
				x_data[entry.offset + i] = vertex.x;
				y_data[entry.offset + i] = vertex.y;
			}
			return entry;
		});
		ListVector::SetListSize(result, total_coords);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------
	static void Register(DatabaseInstance &db) {
		// LINESTRING_2D -> VARCHAR
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::LINESTRING_2D(), LogicalType::VARCHAR,
		                                    BoundCastInfo(ToVarcharCast), 1);
		// LINESTRING_2D -> GEOMETRY
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::LINESTRING_2D(), GeoTypes::GEOMETRY(),
		                                    BoundCastInfo(ToGeometryCast, nullptr, LocalState::InitCast), 1);
		// GEOMETRY -> LINESTRING_2D
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::GEOMETRY(), GeoTypes::LINESTRING_2D(),
		                                    BoundCastInfo(FromGeometryCast, nullptr, LocalState::InitCast), 1);
	}
};

//======================================================================================================================
// POLYGON_2D Casts
//======================================================================================================================

struct PolygonCasts {

	//------------------------------------------------------------------------------------------------------------------
	// POLYGON_2D -> VARCHAR
	//------------------------------------------------------------------------------------------------------------------
	static bool ToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		CoreVectorOperations::Polygon2DToVarchar(source, result, count);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// POLYGON_2D -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool ToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);
		auto &arena = lstate.GetArena();

		auto &ring_vec = ListVector::GetEntry(source);
		const auto ring_entries = ListVector::GetData(ring_vec);
		const auto &coord_vec = ListVector::GetEntry(ring_vec);
		const auto &coord_vec_children = StructVector::GetEntries(coord_vec);
		const auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
		const auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

		UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](const list_entry_t &poly) {
			sgl::geometry geom(sgl::geometry_type::POLYGON, false, false);

			for (idx_t i = 0; i < poly.length; i++) {
				const auto ring_entry = ring_entries[poly.offset + i];

				// Allocate part
				const auto ring_mem = arena.AllocateAligned(sizeof(sgl::geometry));
				const auto ring_ptr = new (ring_mem) sgl::geometry(sgl::geometry_type::LINESTRING);

				// Allocate data
				const auto ring_data_mem = arena.AllocateAligned(sizeof(double) * 2 * ring_entry.length);
				const auto ring_data_ptr = reinterpret_cast<double *>(ring_data_mem);

				for (idx_t j = 0; j < ring_entry.length; j++) {
					ring_data_ptr[j * 2] = x_data[ring_entry.offset + j];
					ring_data_ptr[j * 2 + 1] = y_data[ring_entry.offset + j];
				}

				ring_ptr->set_vertex_data(ring_data_mem, ring_entry.length);

				// Append part
				geom.append_part(ring_ptr);
			}

			return lstate.Serialize(result, geom);
		});
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// GEOMETRY -> POLYGON_2D
	//------------------------------------------------------------------------------------------------------------------
	static bool FromGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);
		auto &ring_vec = ListVector::GetEntry(result);

		idx_t total_rings = 0;
		idx_t total_coords = 0;

		UnaryExecutor::Execute<string_t, list_entry_t>(source, result, count, [&](const string_t &blob) {
			sgl::geometry poly;
			lstate.Deserialize(blob, poly);

			// TODO: Dont throw here, return NULL instead to allow TRY_CAST
			if (poly.get_type() != sgl::geometry_type::POLYGON) {
				throw ConversionException("Cannot cast non-polygon GEOMETRY to POLYGON_2D");
			}

			const auto poly_size = poly.get_count();
			const auto poly_entry = list_entry_t(total_rings, poly_size);

			ListVector::Reserve(result, total_rings + poly_size);

			const auto tail = poly.get_last_part();
			auto head = tail;

			if (head) {
				idx_t ring_idx = 0;
				do {
					D_ASSERT(ring_idx < poly_size);
					head = head->get_next();

					const auto ring_size = head->get_count();
					const auto ring_entry = list_entry_t(total_coords, ring_size);

					ListVector::Reserve(ring_vec, total_coords + ring_size);

					const auto ring_entries = ListVector::GetData(ring_vec);
					auto &coord_vec = ListVector::GetEntry(ring_vec);
					auto &coord_vec_children = StructVector::GetEntries(coord_vec);
					const auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
					const auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

					ring_entries[total_rings + ring_idx] = ring_entry;

					for (idx_t j = 0; j < ring_size; j++) {
						const auto vertext = head->get_vertex_xy(j);
						x_data[ring_entry.offset + j] = vertext.x;
						y_data[ring_entry.offset + j] = vertext.y;
					}
					total_coords += ring_size;

					ring_idx++;
				} while (head != tail);
			}

			total_rings += poly_size;

			return poly_entry;
		});

		ListVector::SetListSize(result, total_rings);
		ListVector::SetListSize(ring_vec, total_coords);

		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------
	static void Register(DatabaseInstance &db) {
		// POLYGON_2D -> VARCHAR
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::POLYGON_2D(), LogicalType::VARCHAR,
		                                    BoundCastInfo(ToVarcharCast), 1);
		// POLYGON_2D -> GEOMETRY
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::POLYGON_2D(), GeoTypes::GEOMETRY(),
		                                    BoundCastInfo(ToGeometryCast, nullptr, LocalState::InitCast), 1);
		// GEOMETRY -> POLYGON_2D
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::GEOMETRY(), GeoTypes::POLYGON_2D(),
		                                    BoundCastInfo(FromGeometryCast, nullptr, LocalState::InitCast), 1);
	}
};

//======================================================================================================================
// BOX_2D Casts
//======================================================================================================================

struct BoxCasts {

	//------------------------------------------------------------------------------------------------------------------
	// BOX_2D -> VARCHAR
	//------------------------------------------------------------------------------------------------------------------
	static bool ToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		CoreVectorOperations::Box2DToVarchar(source, result, count);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// BOX_2D -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool ToGeometryCast2D(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);
		auto &alloc = lstate.GetAllocator();

		using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
		using GEOMETRY_TYPE = PrimitiveType<string_t>;
		GenericExecutor::ExecuteUnary<BOX_TYPE, GEOMETRY_TYPE>(source, result, count, [&](const BOX_TYPE &box) {
			const auto minx = box.a_val;
			const auto miny = box.b_val;
			const auto maxx = box.c_val;
			const auto maxy = box.d_val;

			sgl::geometry poly;
			sgl::polygon::init_from_box(&poly, &alloc, minx, miny, maxx, maxy);

			return lstate.Serialize(result, poly);
		});
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// BOX_2DF -> GEOMETRY
	//------------------------------------------------------------------------------------------------------------------
	static bool ToGeometryCast2F(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &lstate = LocalState::ResetAndGet(parameters);
		auto &alloc = lstate.GetAllocator();
		using BOX_TYPE = StructTypeQuaternary<float, float, float, float>;
		using GEOMETRY_TYPE = PrimitiveType<string_t>;
		GenericExecutor::ExecuteUnary<BOX_TYPE, GEOMETRY_TYPE>(source, result, count, [&](const BOX_TYPE &box) {
			const auto minx = box.a_val;
			const auto miny = box.b_val;
			const auto maxx = box.c_val;
			const auto maxy = box.d_val;

			sgl::geometry poly;
			sgl::polygon::init_from_box(&poly, &alloc, minx, miny, maxx, maxy);

			return lstate.Serialize(result, poly);
		});
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------
	static void Register(DatabaseInstance &db) {
		// BOX_2D -> VARCHAR
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::BOX_2D(), LogicalType::VARCHAR, BoundCastInfo(ToVarcharCast),
		                                    1);

		// BOX_2D -> GEOMETRY
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::BOX_2D(), GeoTypes::GEOMETRY(),
		                                    BoundCastInfo(ToGeometryCast2D, nullptr, LocalState::InitCast), 1);

		// BOX_2F -> GEOMETRY
		ExtensionUtil::RegisterCastFunction(db, GeoTypes::BOX_2DF(), GeoTypes::GEOMETRY(),
		                                    BoundCastInfo(ToGeometryCast2F, nullptr, LocalState::InitCast), 1);
	}
};

} // namespace

//======================================================================================================================
// Vector Operations
//======================================================================================================================
//  TODO: Move/inline this. This is a relic from the original implementation, but being able to access it from outside
//  is not really important anymore (there are other ways to work around it).

//------------------------------------------------------------------------------
// POINT_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Point2DToVarchar(Vector &source, Vector &result, idx_t count) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;

	GenericExecutor::ExecuteUnary<POINT_TYPE, VARCHAR_TYPE>(source, result, count, [&](POINT_TYPE &point) {
		auto x = point.a_val;
		auto y = point.b_val;

		if (std::isnan(x) || std::isnan(y)) {
			return StringVector::AddString(result, "POINT EMPTY");
		}

		return StringVector::AddString(result, StringUtil::Format("POINT (%s)", MathUtil::format_coord(x, y)));
	});
}

//------------------------------------------------------------------------------
// LINESTRING_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::LineString2DToVarchar(Vector &source, Vector &result, idx_t count) {
	auto &inner = ListVector::GetEntry(source);
	auto &children = StructVector::GetEntries(inner);
	auto x_data = FlatVector::GetData<double>(*children[0]);
	auto y_data = FlatVector::GetData<double>(*children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(source, result, count, [&](list_entry_t &line) {
		auto offset = line.offset;
		auto length = line.length;

		if (length == 0) {
			return StringVector::AddString(result, "LINESTRING EMPTY");
		}

		string result_str = "LINESTRING (";
		for (idx_t i = offset; i < offset + length; i++) {
			result_str += MathUtil::format_coord(x_data[i], y_data[i]);
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// POLYGON_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Polygon2DToVarchar(Vector &source, Vector &result, idx_t count) {
	auto &poly_vector = source;
	auto &ring_vector = ListVector::GetEntry(poly_vector);
	auto ring_entries = ListVector::GetData(ring_vector);
	auto &point_vector = ListVector::GetEntry(ring_vector);
	auto &point_children = StructVector::GetEntries(point_vector);
	auto x_data = FlatVector::GetData<double>(*point_children[0]);
	auto y_data = FlatVector::GetData<double>(*point_children[1]);

	UnaryExecutor::Execute<list_entry_t, string_t>(poly_vector, result, count, [&](list_entry_t polygon_entry) {
		auto offset = polygon_entry.offset;
		auto length = polygon_entry.length;

		if (length == 0) {
			return StringVector::AddString(result, "POLYGON EMPTY");
		}

		string result_str = "POLYGON (";
		for (idx_t i = offset; i < offset + length; i++) {
			auto ring_entry = ring_entries[i];
			auto ring_offset = ring_entry.offset;
			auto ring_length = ring_entry.length;
			result_str += "(";
			for (idx_t j = ring_offset; j < ring_offset + ring_length; j++) {
				result_str += MathUtil::format_coord(x_data[j], y_data[j]);
				if (j < ring_offset + ring_length - 1) {
					result_str += ", ";
				}
			}
			result_str += ")";
			if (i < offset + length - 1) {
				result_str += ", ";
			}
		}
		result_str += ")";
		return StringVector::AddString(result, result_str);
	});
}

//------------------------------------------------------------------------------
// BOX_2D -> VARCHAR
//------------------------------------------------------------------------------
void CoreVectorOperations::Box2DToVarchar(Vector &source, Vector &result, idx_t count) {
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using VARCHAR_TYPE = PrimitiveType<string_t>;
	GenericExecutor::ExecuteUnary<BOX_TYPE, VARCHAR_TYPE>(source, result, count, [&](BOX_TYPE &box) {
		return StringVector::AddString(result,
		                               StringUtil::Format("BOX(%s, %s)", MathUtil::format_coord(box.a_val, box.b_val),
		                                                  MathUtil::format_coord(box.c_val, box.d_val)));
	});
}

//------------------------------------------------------------------------------
// GEOMETRY -> VARCHAR
//------------------------------------------------------------------------------
namespace {
class GeometryTextProcessor final : GeometryProcessor<void, bool> {
private:
	string text;

public:
	void OnVertexData(const VertexData &data) {
		auto &dims = data.data;
		auto &strides = data.stride;
		auto count = data.count;

		if (HasZ() && HasM()) {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				auto z = Load<double>(dims[2] + i * strides[2]);
				auto m = Load<double>(dims[3] + i * strides[3]);
				text += MathUtil::format_coord(x, y, z, m);
				if (i < count - 1) {
					text += ", ";
				}
			}
		} else if (HasZ()) {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				auto zm = Load<double>(dims[2] + i * strides[2]);
				text += MathUtil::format_coord(x, y, zm);
				if (i < count - 1) {
					text += ", ";
				}
			}
		} else if (HasM()) {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				auto m = Load<double>(dims[3] + i * strides[3]);
				text += MathUtil::format_coord(x, y, m);
				if (i < count - 1) {
					text += ", ";
				}
			}
		} else {
			for (uint32_t i = 0; i < count; i++) {
				auto x = Load<double>(dims[0] + i * strides[0]);
				auto y = Load<double>(dims[1] + i * strides[1]);
				text += MathUtil::format_coord(x, y);

				if (i < count - 1) {
					text += ", ";
				}
			}
		}
	}

	void ProcessPoint(const VertexData &data, bool in_typed_collection) override {
		if (!in_typed_collection) {
			text += "POINT";
			if (HasZ() && HasM()) {
				text += " ZM";
			} else if (HasZ()) {
				text += " Z";
			} else if (HasM()) {
				text += " M";
			}
			text += " ";
		}

		if (data.count == 0) {
			text += "EMPTY";
		} else if (in_typed_collection) {
			OnVertexData(data);
		} else {
			text += "(";
			OnVertexData(data);
			text += ")";
		}
	}

	void ProcessLineString(const VertexData &data, bool in_typed_collection) override {
		if (!in_typed_collection) {
			text += "LINESTRING";
			if (HasZ() && HasM()) {
				text += " ZM";
			} else if (HasZ()) {
				text += " Z";
			} else if (HasM()) {
				text += " M";
			}
			text += " ";
		}

		if (data.count == 0) {
			text += "EMPTY";
		} else {
			text += "(";
			OnVertexData(data);
			text += ")";
		}
	}

	void ProcessPolygon(PolygonState &state, bool in_typed_collection) override {
		if (!in_typed_collection) {
			text += "POLYGON";
			if (HasZ() && HasM()) {
				text += " ZM";
			} else if (HasZ()) {
				text += " Z";
			} else if (HasM()) {
				text += " M";
			}
			text += " ";
		}

		if (state.RingCount() == 0) {
			text += "EMPTY";
		} else {
			text += "(";
			bool first = true;
			while (!state.IsDone()) {
				if (!first) {
					text += ", ";
				}
				first = false;
				text += "(";
				auto vertices = state.Next();
				OnVertexData(vertices);
				text += ")";
			}
			text += ")";
		}
	}

	void ProcessCollection(CollectionState &state, bool) override {
		bool collection_is_typed = false;
		switch (CurrentType()) {
		case GeometryType::MULTIPOINT:
			text += "MULTIPOINT";
			collection_is_typed = true;
			break;
		case GeometryType::MULTILINESTRING:
			text += "MULTILINESTRING";
			collection_is_typed = true;
			break;
		case GeometryType::MULTIPOLYGON:
			text += "MULTIPOLYGON";
			collection_is_typed = true;
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			text += "GEOMETRYCOLLECTION";
			collection_is_typed = false;
			break;
		default:
			throw InvalidInputException("Invalid geometry type");
		}

		if (HasZ() && HasM()) {
			text += " ZM";
		} else if (HasZ()) {
			text += " Z";
		} else if (HasM()) {
			text += " M";
		}

		if (state.ItemCount() == 0) {
			text += " EMPTY";
		} else {
			text += " (";
			bool first = true;
			while (!state.IsDone()) {
				if (!first) {
					text += ", ";
				}
				first = false;
				state.Next(collection_is_typed);
			}
			text += ")";
		}
	}

	virtual ~GeometryTextProcessor() = default;

	const string &Execute(const geometry_t &geom) {
		text.clear();
		Process(geom, false);
		return text;
	}
};

} // namespace

void CoreVectorOperations::GeometryToVarchar(Vector &source, Vector &result, idx_t count) {
	GeometryTextProcessor processor;
	UnaryExecutor::Execute<geometry_t, string_t>(source, result, count, [&](const geometry_t &input) {
		const auto text = processor.Execute(input);
		return StringVector::AddString(result, text);
	});
}

//######################################################################################################################
// Register
//######################################################################################################################

void RegisterSpatialCastFunctions(DatabaseInstance &db) {
	GeometryCasts::Register(db);
	PointCasts::Register(db);
	LinestringCasts::Register(db);
	PolygonCasts::Register(db);
	BoxCasts::Register(db);
}

} // namespace duckdb
