#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"

#include "duckdb/common/constants.hpp"

namespace spatial {

namespace core {

static void TileEnvelopeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	using INT_TYPE = PrimitiveType<int32_t>;
	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;

	const auto RADIUS = 6378137.0;
	const auto CIRCUMFERENCE = 2 * PI * RADIUS;

	GenericExecutor::ExecuteTernary<INT_TYPE, INT_TYPE, INT_TYPE, BOX_TYPE>(
	    args.data[0], args.data[1], args.data[2], result, count,
	    [&](const INT_TYPE z, const INT_TYPE x, const INT_TYPE y) {
		    const auto tile_size = CIRCUMFERENCE / pow(2, z.val);

		    const auto left = x.val * tile_size - CIRCUMFERENCE / 2;
		    const auto right = left + tile_size;

		    const auto top = CIRCUMFERENCE / 2 - y.val * tile_size;
		    const auto bottom = top - tile_size;

		    return BOX_TYPE { left, bottom, right, top };
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};
//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStTileEnvelope(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_TileEnvelope");

	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER},
		GeoTypes::BOX_2D(), TileEnvelopeFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_TileEnvelope", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial
