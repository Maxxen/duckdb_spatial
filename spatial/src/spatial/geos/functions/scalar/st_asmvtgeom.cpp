#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/functions/scalar.hpp"

namespace spatial {

namespace geos {

using namespace core;

//------------------------------------------------------------------------------
// GEOMETRY -> GEOMETRY
//------------------------------------------------------------------------------
static unique_ptr<FunctionData> GeometryAsMVTBind(ClientContext &context, ScalarFunction &bound_function,
	vector<unique_ptr<Expression>> &arguments) {

	// Default optional arguments
	if(arguments.size() < 3) {
		// Extent
		arguments.push_back(make_uniq_base<Expression, BoundConstantExpression>(Value::INTEGER(4096)));
	}
	if(arguments.size() < 4) {
		// Buffer
		arguments.push_back(make_uniq_base<Expression, BoundConstantExpression>(Value::INTEGER(256)));
	}
	if(arguments.size() < 5) {
		// Clip
		arguments.push_back(make_uniq_base<Expression, BoundConstantExpression>(Value::BOOLEAN(true)));
	}

	return nullptr;
}

static void GeometryAsMVTFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();

	UnifiedVectorFormat geo_vec;
	UnifiedVectorFormat bnd_vec;
	UnifiedVectorFormat ext_vec;
	UnifiedVectorFormat buf_vec;
	UnifiedVectorFormat clp_vec;

	UnifiedVectorFormat xmin_vec;
	UnifiedVectorFormat ymin_vec;
	UnifiedVectorFormat xmax_vec;
	UnifiedVectorFormat ymax_vec;

	args.data[0].ToUnifiedFormat(count, geo_vec);
	args.data[1].ToUnifiedFormat(count, bnd_vec);
	args.data[2].ToUnifiedFormat(count, ext_vec);
	args.data[3].ToUnifiedFormat(count, buf_vec);
	args.data[4].ToUnifiedFormat(count, clp_vec);

	auto &bounds_elements = StructVector::GetEntries(args.data[1]);
	bounds_elements[0]->ToUnifiedFormat(count, xmin_vec);
	bounds_elements[1]->ToUnifiedFormat(count, ymin_vec);
	bounds_elements[2]->ToUnifiedFormat(count, xmax_vec);
	bounds_elements[3]->ToUnifiedFormat(count, ymax_vec);

	for(idx_t out_idx = 0; out_idx < count; out_idx++) {
		const auto geo_idx = geo_vec.sel->get_index(out_idx);
		const auto bnd_idx = bnd_vec.sel->get_index(out_idx);
		const auto ext_idx = ext_vec.sel->get_index(out_idx);
		const auto buf_idx = buf_vec.sel->get_index(out_idx);
		const auto clp_idx = clp_vec.sel->get_index(out_idx);

		const auto geo_valid= geo_vec.validity.RowIsValid(geo_idx);
		const auto bnd_valid= bnd_vec.validity.RowIsValid(bnd_idx);
		const auto ext_valid= ext_vec.validity.RowIsValid(ext_idx);
		const auto buf_valid= buf_vec.validity.RowIsValid(buf_idx);
		const auto clp_valid= clp_vec.validity.RowIsValid(clp_idx);

		if(!geo_valid || !bnd_valid || !ext_valid || !buf_valid || !clp_valid) {
			FlatVector::SetNull(result, out_idx, true);
			continue;
		}

		const auto &blob = UnifiedVectorFormat::GetData<geometry_t>(geo_vec)[geo_idx];

		if(blob.GetType() == GeometryType::GEOMETRYCOLLECTION) {
			// Do not support geometry collections
			FlatVector::SetNull(result, out_idx, true);
			continue;;
		}

		auto geom = lstate.ctx.Deserialize(blob);

		// Orient all polygons so the exerior ring is clockwise
		GEOSOrientPolygons_r(ctx, geom.get(), 1);

		const auto xmin_idx = xmin_vec.sel->get_index(out_idx);
		const auto ymin_idx = ymin_vec.sel->get_index(out_idx);
		const auto xmax_idx = xmax_vec.sel->get_index(out_idx);
		const auto ymax_idx = ymax_vec.sel->get_index(out_idx);

		const auto xmin = UnifiedVectorFormat::GetData<double>(xmin_vec)[xmin_idx];
		const auto ymin = UnifiedVectorFormat::GetData<double>(ymin_vec)[ymin_idx];
		const auto xmax = UnifiedVectorFormat::GetData<double>(xmax_vec)[xmax_idx];
		const auto ymax = UnifiedVectorFormat::GetData<double>(ymax_vec)[ymax_idx];

		const auto ext = UnifiedVectorFormat::GetData<int32_t>(ext_vec)[ext_idx];
		const auto buf = UnifiedVectorFormat::GetData<int32_t>(buf_vec)[buf_idx];

		const auto tile_w = xmax - xmin;
		const auto tile_h = ymax - ymin;

		const auto &should_clip = UnifiedVectorFormat::GetData<bool>(clp_vec)[clp_idx];
		if(should_clip) {

			const auto buf_w = buf * tile_w / ext;
			const auto buf_h = buf * tile_h / ext;

			const auto buf_xmin = xmin - buf_w;
			const auto buf_ymin = ymin - buf_h;
			const auto buf_xmax = xmax + buf_w;
			const auto buf_ymax = ymax + buf_h;

			auto clipped = make_uniq_geos(ctx,
				GEOSClipByRect_r(ctx, geom.get(), buf_xmin, buf_ymin, buf_xmax, buf_ymax));

			if(GEOSGeomTypeId_r(ctx, clipped.get()) == GEOS_GEOMETRYCOLLECTION) {
				// No geometry left after clipping, return null
				FlatVector::SetNull(result, out_idx, true);
				continue;
			}

			geom = std::move(clipped);
		}

		// Scale the geometry to fit the extent
		// minx, miny, scalex, scaley
		double transform_data[4] = {xmin, ymin, tile_w / (xmax - xmin), tile_h / (ymax - ymin)};
		auto scaled_geom = make_uniq_geos(ctx,
			GEOSGeom_transformXY_r(ctx, geom.get(), [](double *x, double *y, void *arg) {
			const auto data = static_cast<double *>(arg);
			*x = (*x - data[0]) * data[2];
			*y = (*y - data[1]) * data[3];
			return 1;
		}, &transform_data));

		// Serialize the geometry
		FlatVector::GetData<geometry_t>(result)[out_idx] = lstate.ctx.Serialize(result, scaled_geom);
	}

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
)";

static constexpr const char *DOC_EXAMPLE = R"(
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStAsMVTGeom(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_AsMVTGeom");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::BOX_2D()}, GeoTypes::GEOMETRY(), GeometryAsMVTFunction, GeometryAsMVTBind, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::BOX_2D(), LogicalType::INTEGER}, GeoTypes::GEOMETRY(), GeometryAsMVTFunction, GeometryAsMVTBind, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::BOX_2D(), LogicalType::INTEGER, LogicalType::INTEGER}, GeoTypes::GEOMETRY(), GeometryAsMVTFunction, GeometryAsMVTBind, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::BOX_2D(), LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BOOLEAN}, GeoTypes::GEOMETRY(), GeometryAsMVTFunction, GeometryAsMVTBind, nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_AsMVTGeom", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial