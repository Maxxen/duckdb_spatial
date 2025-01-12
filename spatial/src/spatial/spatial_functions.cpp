#include "duckdb/common/assert.hpp"
#include "spatial/core/function_builder.hpp"
#include "spatial/core/types.hpp"
#include "spatial/spatial_geometry.hpp"

#include <spatial/core/functions/scalar.hpp>

namespace spatial {

using namespace geometry;
using namespace core;

//------------------------------------------------------------------------------
// DuckDB Arena
//------------------------------------------------------------------------------
namespace {

// Wrap DuckDB's ArenaAllocator
class DuckDBArena final : public geometry::Arena {
public:
	explicit DuckDBArena(ArenaAllocator &allocator) : allocator(allocator) {}
	void* Allocate(size_t size) override;
	void* Reallocate(void *ptr, size_t old_size, size_t new_size) override;
	void Reset() override;
protected:
	ArenaAllocator &allocator;
};

inline void* DuckDBArena::Allocate(size_t size) {
	return allocator.AllocateAligned(size);
}

inline void* DuckDBArena::Reallocate(void *ptr, size_t old_size, size_t new_size) {
	return allocator.ReallocateAligned(data_ptr_cast(ptr), old_size, new_size);
}

inline void DuckDBArena::Reset() {
	allocator.Reset();
}

}

//------------------------------------------------------------------------------
// Serialized
//------------------------------------------------------------------------------
struct geom_t {
	static void CheckVersion(const string_t &str) {
		// return Serde::V1::CheckVersion(str);
		// TODO: Implement this
	}
	static GeometryType GetType(const string_t &str) {
		CheckVersion(str);

		GeometryType res;
		memcpy(&res, str.GetPrefix(), sizeof(GeometryType));
		return res;
	}
};

//------------------------------------------------------------------------------
// Local State
//------------------------------------------------------------------------------
namespace {

class LocalState final : public FunctionLocalState {
public:
	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
		return make_uniq<LocalState>(state.GetContext());
	}

	static LocalState& ResetAndGet(ExpressionState &state) {
		auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<LocalState>();
		return local_state;
	}

	// TODO: This where we will catch geometry de/serialization exceptions and rethrow duckdb exceptions
	// TODO: This is where we will inspect the client context settings to configure serialization options
	Geometry Deserialize(const string_t &blob);
	string_t Serialize(Vector &result, const Geometry &geom) const;

	explicit LocalState(ClientContext &context) :
		allocator(BufferAllocator::Get(context)), arena(allocator) {
	}
private:
	ArenaAllocator allocator;
	DuckDBArena arena;
};

string_t LocalState::Serialize(Vector &result, const Geometry &geom) const {

	// Get the size of the serialized geometry
	const auto size = Serde::V1::GetRequiredSize(&geom, geom.GetVertexSize());

	// Allocate a blob of the correct size
	auto blob = StringVector::EmptyString(result, size);
	const auto ptr = data_ptr_cast(blob.GetDataWriteable());

	// Serialize the geometry into the blob
	Serde::V1::Serialize(&geom, ptr, size);

	// Finalize and return the blob
	blob.Finalize();
	return blob;
}

Geometry LocalState::Deserialize(const string_t &blob) {
	const auto blob_ptr = const_data_ptr_cast(blob.GetDataUnsafe());
	const auto blob_len = blob.GetSize();

	return Serde::V1::Deserialize(arena, blob_ptr, blob_len);
}

} // namespace

//------------------------------------------------------------------------------
// Functions
//------------------------------------------------------------------------------

namespace {

struct ST_Area {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
			const auto geom = lstate.Deserialize(geom_blob);
			return operations::GeometryGetArea(geom);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Area", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::DOUBLE);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "property");

			// TODO: Set description
		});
	}
};

/*
struct ST_AsGeoJSON {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);

		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
			const auto geom = lstate.Deserialize(geom_blob);
			const auto json = mg_geom_to_geojson(lstate.GetArena(), &geom);

			// TODO: We dont need to copy the string like this, just return a string_t
			return StringVector::AddString(result, json);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_AsGeoJSON", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {
				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::VARCHAR);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "conversion");

			// TODO: Set description
		});
	}
};
*/

/*
struct ST_AsText {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
			const auto geom = lstate.Deserialize(geom_blob);
			const auto text = mg_geom_to_wkt(lstate.GetArena(), &geom);
			// TODO: We dont need to copy the string like this, just return a string_t
			return StringVector::AddString(result, text);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_AsText", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::VARCHAR);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "conversion");
		});
	}

};
struct ST_AsWKB {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = LocalState::ResetAndGet(state);
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
		const auto geom = lstate.Deserialize(geom_blob);
		const auto wkb = mg_geom_to_wkb(lstate.GetArena(), &geom);
		// TODO: We dont need to copy the string like this, just return a string_t
		return StringVector::AddString(result, wkb);
	});
}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_AsWKB", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			variant.AddParameter("geom", GeoTypes::GEOMETRY());
			variant.SetReturnType(LogicalType::BLOB);

			variant.SetFunction(Execute);
			variant.SetInit(LocalState::Init);
		});

		func.SetTag("ext", "spatial");
		func.SetTag("category", "conversion");
		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_AsHEXWKB {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
			const auto geom = lstate.Deserialize(geom_blob);
			const auto hexwkb = mg_geom_to_hexwkb(lstate.GetArena(), &geom);

			// TODO: We dont need to copy the string like this, just return a string_t
			return StringVector::AddString(result, hexwkb);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_AsHEXWKB", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::VARCHAR);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "conversion");
		});
	}

};
struct ST_AsSVG {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = LocalState::ResetAndGet(state);
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
		const auto geom = lstate.Deserialize(geom_blob);
		const auto svg = mg_geom_to_svg(lstate.GetArena(), &geom);
		// TODO: We dont need to copy the string like this, just return a string_t
		return StringVector::AddString(result, svg);
	});
}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_AsSVG", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			variant.AddParameter("geom", GeoTypes::GEOMETRY());
			variant.SetReturnType(LogicalType::VARCHAR);

			variant.SetFunction(Execute);
			variant.SetInit(LocalState::Init);
		});

		func.SetTag("ext", "spatial");
		func.SetTag("category", "conversion");
		// TODO: Set description
	});
}

};
*/
struct ST_Centroid {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Centroid", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::VARCHAR);

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}
};

struct ST_Collect {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);

		const auto count = args.size();
		auto &child_vec = ListVector::GetEntry(args.data[0]);

		UnifiedVectorFormat child_format;
		child_vec.ToUnifiedFormat(count, child_format);
		const auto child_data = UnifiedVectorFormat::GetData<string_t>(child_format);

		UnaryExecutor::Execute<list_entry_t, string_t>(args.data[0], result, count, [&](const list_entry_t &geom_list) {
			const auto offset = geom_list.offset;
			const auto length = geom_list.length;

			auto has_z = false;
			auto has_m = false;

			auto all_points = true;
			auto all_lines = true;
			auto all_polygons = true;

			// First figure out if we have Z or M
			for (auto geom_idx = offset; geom_idx < offset + length; geom_idx++) {
				const auto mapped_idx = child_format.sel->get_index(geom_idx);

				if (!child_format.validity.RowIsValid(mapped_idx)) {
					continue;
				}

				const auto &geom_blob = child_data[mapped_idx];
				const auto props = geom_t::GetProperties(geom_blob);
				has_z = has_z || props.HasZ();
				has_m = has_m || props.HasM();
			}

			// Now collect the geometries
			Geometry collection(GeometryType::MULTI_GEOMETRY);

			for (auto geom_idx = offset; geom_idx < offset + length; geom_idx++) {
				const auto mapped_idx = child_format.sel->get_index(geom_idx);

				if (!child_format.validity.RowIsValid(mapped_idx)) {
					continue;
				}

				const auto &geom_blob = child_data[mapped_idx];
				const auto geom = lstate.Deserialize(geom_blob);

				// TODO: Check empty recursively
				if(geom.IsEmpty()) {
					continue;
				}

				const auto geom_type = geom.GetType();
				all_points = all_points && geom_type == GeometryType::POINT;
				all_lines = all_lines && geom_type == GeometryType::LINESTRING;
				all_polygons = all_polygons && geom_type == GeometryType::POLYGON;

				// Ensure all geometries have the same vertex type;
				geom.SetVertexType(lstate.arena, has_z, has_m);

				// Allocate a slot for this geometry and push it to the collection
				auto ptr = lstate.GetArena().Make<Geometry>(geom);
				collection.AppendPart(ptr);
			}

			// If its not empty, attempt to specialize the collection
			if(!collection.IsEmpty()) {
				if(all_points) {
					collection.SetTypeUnsafe(GeometryType::MULTI_POINT);
				} else if(all_lines) {
					collection.SetTypeUnsafe(GeometryType::MULTI_LINESTRING);
				} else if(all_polygons) {
					collection.SetTypeUnsafe(GeometryType::MULTI_POLYGON);
				}
			}

			return lstate.Serialize(result, collection);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Collect", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom_list", LogicalType::LIST(GeoTypes::GEOMETRY()));
				variant.SetReturnType(GeoTypes::GEOMETRY());

				variant.SetInit(LocalState::Init);
				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}
};


/*
struct ST_CollectionExtract {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_CollectionExtract", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Contains {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Contains", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};

*/

struct ST_Dimension {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		UnaryExecutor::Execute<string_t, int32_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
			const auto geom = lstate.Deserialize(geom_blob);
			return operations::GeometryGetDimension(geom);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Dimension", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::INTEGER);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "property");
		});
	}
};

struct ST_Distance {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Distance", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type
				variant.SetReturnType(LogicalType::DOUBLE);
				variant.AddParameter("geom1", GeoTypes::GEOMETRY());

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}
};

struct ST_Dump {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		auto &geom_vec = args.data[0];

		const auto count = args.size();

		UnifiedVectorFormat geom_format;
		geom_vec.ToUnifiedFormat(count, geom_format);

		idx_t total_geom_count = 0;
		idx_t total_path_count = 0;

		vector<std::tuple<Geometry*, vector<int32_t>>> stack;
		vector<std::tuple<Geometry*, vector<int32_t>>> items;

		for(idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {
			const auto in_row_idx = geom_format.sel->get_index(out_row_idx);

			if(!geom_format.validity.RowIsValid(in_row_idx)) {
				FlatVector::SetNull(result, out_row_idx, true);
				continue;
			}

			// Reset the stack and items
			stack.clear();
			items.clear();

			const auto &geom_blob = UnifiedVectorFormat::GetData<string_t>(geom_format)[in_row_idx];
			auto geom = lstate.Deserialize(geom_blob);

			stack.emplace_back(&geom, vector<int32_t>());

			while(stack.empty()) {
				auto current = stack.back();
				const auto current_geom = std::get<0>(current);
				const auto &current_path = std::get<1>(current);

				stack.pop_back();

				if(current_geom->IsCollection()) {
					int32_t i = 0;
					for(const auto part : *current_geom) {
						auto path = current_path;
						path.push_back(i + 1); // path is 1-indexed
						stack.emplace_back(part, path);
						i++;
					}
				} else {
					items.push_back(current);
				}
			}

			// Reverse the results
			std::reverse(items.begin(), items.end());

			// Push the result vector as a list
			const auto result_entries = ListVector::GetData(result);

			const auto geom_offset = total_geom_count;
			const auto geom_length = items.size();

			result_entries[out_row_idx].offset = geom_offset;
			result_entries[out_row_idx].length = geom_length;

			total_geom_count += geom_length;

			ListVector::Reserve(result, total_geom_count);
			ListVector::SetListSize(result, total_geom_count);

			auto &result_list = ListVector::GetEntry(result);
			auto &result_list_children = StructVector::GetEntries(result_list);
			auto &result_geom_vec = *result_list_children[0];
			auto &result_path_vec = *result_list_children[1];

			const auto result_geom_data = FlatVector::GetData<string_t>(result_geom_vec);

			for(idx_t geom_idx = 0; geom_idx < geom_length; geom_idx++) {
				// Serialize the geometry
				const auto item_blob = std::get<0>(items[geom_idx]);
				result_geom_data[geom_offset + geom_idx] = lstate.Serialize(result_geom_vec, *item_blob);

				// Now write the paths
				const auto &path = std::get<1>(items[geom_idx]);

				const auto path_offset = total_path_count;
				const auto path_length = path.size();

				total_path_count += path_length;

				ListVector::Reserve(result_path_vec, total_path_count);
				ListVector::SetListSize(result_path_vec, total_path_count);

				const auto path_entries = ListVector::GetData(result_path_vec);

				path_entries[geom_offset + geom_idx].offset = path_offset;
				path_entries[geom_offset + geom_idx].length = path_length;

				auto &path_data_vec = ListVector::GetEntry(result_path_vec);
				const auto path_data = FlatVector::GetData<int32_t>(path_data_vec);

				for(idx_t path_idx = 0; path_idx < path_length; path_idx++) {
					path_data[path_offset + path_idx] = path[path_idx];
				}
			}
		}

		if(count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Dump", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());

				variant.SetReturnType(
					LogicalType::LIST(
					LogicalType::STRUCT({
						{"geom", GeoTypes::GEOMETRY()},
						{"path", LogicalType::LIST(LogicalType::INTEGER)}})));

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "construction");
		});
	}
};

struct ST_EndPoint {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		auto &geom_vec = args.data[0];
		const auto count = args.size();

		UnaryExecutor::ExecuteWithNulls<string_t, string_t>(geom_vec, result, count,
			[&](const string_t &geom, ValidityMask &mask, const idx_t row_idx) {

				if(geom_t::GetType(geom) != GeometryType::LINESTRING) {
					mask.SetInvalid(row_idx);
					return string_t{};
				}

				const auto line = lstate.Deserialize(geom);
				const auto point_count = line.GetSize();

				if(point_count == 0) {
					mask.SetInvalid(row_idx);
					return string_t{};
				}

				const auto point = operations::GetPointFromLine(line, point_count - 1);
				return lstate.Serialize(result, point);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_EndPoint", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(GeoTypes::GEOMETRY());

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "construction");
		});
	}

};
struct ST_Extent {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Extent", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_ExteriorRing {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_ExteriorRing", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_FlipCoordinates {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_FlipCoordinates", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Force {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Force", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_GeometryType {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_GeometryType", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_GeomFromHEXWKB {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_GeomFromHEXWKB", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_GeomFromText {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_GeomFromText", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_GeomFromWKB {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_GeomFromWKB", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Has {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Has", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Haversine {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Haversine", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Hilbert {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Hilbert", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Intersects {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Intersects", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type
			variant.SetReturnType(LogicalType::DOUBLE);
			variant.AddParameter("geom1", GeoTypes::GEOMETRY());


			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_IntersectsExtent {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_IntersectsExtent", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_IsClosed {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_IsClosed", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_IsEmpty {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_IsEmpty", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Length {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Length", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_MakeEnvelope {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_MakeEnvelope", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_MakeLine {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_MakeLine", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_MakePolygon {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_MakePolygon", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_Multi {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_Multi", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_NGeometries {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_NGeometries", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_NInteriorRings {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_NInteriorRings", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}

};

struct ST_NPoints {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		UnaryExecutor::Execute<string_t, uint32_t>(args.data[0], result, args.size(), [&](const string_t &geom_blob) {
			const auto geom = lstate.Deserialize(geom_blob);
			return operations::GeometryGetNumVertices(geom);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_NPoints", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::UINTEGER);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "property");

			func.SetDescription("Returns the number of vertices in a geometry");
		});
	}
};

struct ST_Perimeter {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Perimeter", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}

};

struct ST_Point {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

		// TODO: Dont initialize allocator here, we dont need it.
		auto &lstate = LocalState::ResetAndGet(state);

		const auto count = args.size();
		auto &x_vec = args.data[0];
		auto &y_vec = args.data[1];

		BinaryExecutor::Execute<double, double, string_t>(x_vec, y_vec, result, count, [&](double x, double y) {
			const double buf[2] = { x, y };

			Geometry geom(GeometryType::POINT);
			geom.SetVertices(reinterpret_cast<const uint8_t*>(buf), 1);

			return lstate.Serialize(result, geom);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Point", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("x", LogicalType::DOUBLE);
				variant.AddParameter("y", LogicalType::DOUBLE);
				variant.SetReturnType(GeoTypes::GEOMETRY());

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "construction");
		});
	}

};
struct ST_PointN {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_PointN", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}

};
struct ST_Points {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_Points", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}

};

struct ST_QuadKey {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_QuadKey", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}

};
struct ST_RemoveRepeatedPoints {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_RemoveRepeatedPoints", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type
				variant.SetReturnType(LogicalType::DOUBLE);
				variant.AddParameter("geom1", GeoTypes::GEOMETRY());


				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}
};

struct ST_StartPoint {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);
		auto &geom_vec = args.data[0];
		const auto count = args.size();

		UnaryExecutor::ExecuteWithNulls<string_t, string_t>(geom_vec, result, count,
			[&](const string_t &geom, ValidityMask &mask, const idx_t row_idx) {

				if(geom_t::GetType(geom) != GeometryType::LINESTRING) {
					mask.SetInvalid(row_idx);
					return string_t{};
				}

				const auto line = lstate.Deserialize(geom);
				if(line.IsEmpty()) {
					mask.SetInvalid(row_idx);
					return string_t{};
				}

				const auto point = operations::GetPointFromLine(line, 0);
				return lstate.Serialize(result, point);
		});
	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_StartPoint", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(GeoTypes::GEOMETRY());

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "construction");
		});
	}

};

template<class OP>
struct BasePointAccessFunction {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &lstate = LocalState::ResetAndGet(state);

		UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(),
			[&](const string_t &blob) {
			auto geom = lstate.Deserialize(blob);

			/*
			if(geom.get_type() != geometry::GeometryType::POINT) {
				throw InvalidInputException("%s requires a POINT geometry", OP::NAME);
			}
			*/

			const auto offset = OP::ORDINATE * sizeof(double);

			double ordinate;
			// memcpy(&ordinate, geom.shape.verts.ptr + offset, sizeof(double));
			return ordinate;
		});
	}


	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, OP::NAME, [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				variant.AddParameter("geom", GeoTypes::GEOMETRY());
				variant.SetReturnType(LogicalType::DOUBLE);

				variant.SetFunction(Execute);
				variant.SetInit(LocalState::Init);

				variant.SetExample(OP::EXAMPLE);
				variant.SetDescription(OP::DESCRIPTION);
			});

			func.SetTag("ext", "spatial");
			func.SetTag("category", "property");
		});
	}
};

struct ST_X : BasePointAccessFunction<ST_X> {
	static constexpr auto NAME = "ST_X";
	static constexpr auto ORDINATE = 0;
	static constexpr auto DESCRIPTION = "Returns the X ordinate of the point";
	static constexpr auto EXAMPLE = "SELECT ST_X(ST_Point(1.5, 2.5))";
};


struct ST_Y : BasePointAccessFunction<ST_Y> {
	static constexpr auto NAME = "ST_Y";
	static constexpr auto ORDINATE = 1;
	static constexpr auto DESCRIPTION = "Returns the Y ordinate of the point";
	static constexpr auto EXAMPLE = "SELECT ST_Y(ST_Point(1.5, 2.5))";
};


struct ST_Z : BasePointAccessFunction<ST_Z> {
	static constexpr auto NAME = "ST_Z";
	static constexpr auto ORDINATE = 2;
	static constexpr auto DESCRIPTION = "Returns the Z ordinate of the point";
	static constexpr auto EXAMPLE = "SELECT ST_Z(ST_Point(1.5, 2.5, 3.5))";
};


struct ST_M : BasePointAccessFunction<ST_M> {
	static constexpr auto NAME = "ST_M";
	static constexpr auto ORDINATE = 3;
	static constexpr auto DESCRIPTION = "Returns the M ordinate of the point";
	static constexpr auto EXAMPLE = "SELECT ST_M(ST_Point(1.5, 2.5, 3.5, 4.5))";
};

template<class OP>
struct BaseVertexAggregateFunction {
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}
};

struct ST_XMax : BaseVertexAggregateFunction<ST_XMax> {

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

	}

	static void Register(DatabaseInstance &db) {
		FunctionBuilder::RegisterScalar(db, "ST_XMax", [&](ScalarFunctionBuilder &func) {
			func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

				// TODO: Set params
				// TODO: Set return type

				variant.SetFunction(Execute);
			});

			// TODO: Set tags
			// TODO: Set description
		});
	}
};


struct ST_XMin : BaseVertexAggregateFunction<ST_XMin> {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_XMin", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};

struct ST_YMax : BaseVertexAggregateFunction<ST_YMax> {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_YMax", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_YMin : BaseVertexAggregateFunction<ST_YMin> {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_YMin", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};

struct ST_ZMax : BaseVertexAggregateFunction<ST_ZMax> {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_ZMax", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_ZMin : BaseVertexAggregateFunction<ST_ZMin>{

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_ZMin", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};

struct ST_MMax : BaseVertexAggregateFunction<ST_MMax> {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_MMax", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};
struct ST_MMin : BaseVertexAggregateFunction<ST_MMin> {

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {

}

static void Register(DatabaseInstance &db) {
	FunctionBuilder::RegisterScalar(db, "ST_MMin", [&](ScalarFunctionBuilder &func) {
		func.AddVariant([&](ScalarFunctionVariantBuilder &variant) {

			// TODO: Set params
			// TODO: Set return type

			variant.SetFunction(Execute);
		});

		// TODO: Set tags
		// TODO: Set description
	});
}

};

}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void ::spatial::core::CoreScalarFunctions::Register(DatabaseInstance &db) {
	ST_Area::Register(db);
	// ST_AsGeoJSON::Register(db);
	// ST_AsText::Register(db);
	// ST_AsWKB::Register(db);
	// ST_AsHEXWKB::Register(db);
	// ST_AsSVG::Register(db);
	ST_Centroid::Register(db);
	ST_Collect::Register(db);
	// ST_CollectionExtract::Register(db);
	// ST_Contains::Register(db);
	ST_Dimension::Register(db);
	ST_Distance::Register(db);
	ST_Dump::Register(db);

	ST_EndPoint::Register(db);
	//ST_Extent::Register(db);
	//ST_ExteriorRing::Register(db);
	//ST_FlipCoordinates::Register(db);
	//ST_Force::Register(db);
	//ST_GeometryType::Register(db);
	//ST_GeomFromHEXWKB::Register(db);
	//ST_GeomFromText::Register(db);
	//ST_GeomFromWKB::Register(db);
	//ST_Has::Register(db);
	//ST_Haversine::Register(db);
	//ST_Hilbert::Register(db);
	ST_Intersects::Register(db);
	//ST_IntersectsExtent::Register(db);
	//ST_IsClosed::Register(db);
	//ST_IsEmpty::Register(db);
	//ST_Length::Register(db);
	//ST_MakeEnvelope::Register(db);
	//ST_MakeLine::Register(db);
	//ST_MakePolygon::Register(db);
	//ST_Multi::Register(db);
	//ST_NGeometries::Register(db);
	//ST_NInteriorRings::Register(db);
	ST_NPoints::Register(db);
	//ST_Perimeter::Register(db);
	ST_Point::Register(db);
	//ST_PointN::Register(db);
	//ST_Points::Register(db);
	//ST_QuadKey::Register(db);
	ST_RemoveRepeatedPoints::Register(db);
	ST_StartPoint::Register(db);
	//ST_X::Register(db);
	//ST_XMax::Register(db);
	//ST_XMin::Register(db);
	//ST_Y::Register(db);
	//ST_YMax::Register(db);
	//ST_YMin::Register(db);
	//ST_Z::Register(db);
	//ST_ZMax::Register(db);
	//ST_ZMin::Register(db);
	//ST_M::Register(db);
	//ST_MMax::Register(db);
	//ST_MMin::Register(db);
}

} // namespace spatial

