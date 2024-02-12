#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY -> SVG
//------------------------------------------------------------------------------

static void SetDefaultStyle(case_insensitive_map_t<string> style, GeometryType type) {
    switch(type) {
        case GeometryType::POINT:
            style["r"] = "0.1";
            break;
        case GeometryType::LINESTRING:
            style["fill"] = "none";
            style["stroke"] = "black";
            style["stroke-width"] = "0.01";
            break;
        default:
            break;
    }
}

static string GeometryToSVG(const Geometry &geom, const case_insensitive_map_t<string> &style) {

    string attributes;
    for (auto &entry : style) {
        attributes += StringUtil::Format("%s=\"%s\" ", entry.first.c_str(), entry.second.c_str());
    }

    switch(geom.Type()) {
        case GeometryType::POINT: {
            auto point = geom.GetPoint();
            auto vertex = point.GetVertex();
            return StringUtil::Format(R"(<circle cx="%f" cy="%f" %s/>)", vertex.x, vertex.y, attributes);
        }
        case GeometryType::LINESTRING: {
            auto linestring = geom.GetLineString();
            string result = StringUtil::Format("<polyline %s points=\"", attributes);
            for (idx_t i = 0; i < linestring.Count(); i++) {
                auto vertex = linestring.Vertices().Get(i);
                result += StringUtil::Format("%f,%f ", vertex.x, vertex.y);
            }
            result += "\"/>";
            return result;
        }
        default:
            throw NotImplementedException("SVG for geometry type not implemented");
    }
}

static void GeometryAsSVGFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &geom_vec = args.data[0];
    auto &map_vec = args.data[1];
    auto &key_vec = MapVector::GetKeys(map_vec);
    auto &value_vec = MapVector::GetValues(map_vec);
    auto key_data = FlatVector::GetData<string_t>(key_vec);
    auto value_data = FlatVector::GetData<string_t>(value_vec);

	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

    case_insensitive_map_t<string> style;

	BinaryExecutor::Execute<string_t, list_entry_t, string_t>(geom_vec, map_vec, result, count, [&](string_t input, list_entry_t properties) {
        // Deserialize the geometry
        auto geometry = lstate.factory.Deserialize(input);

        // Reset the style
        style.clear();

        // Set defaults
        SetDefaultStyle(style, geometry.Type());

        // Set the style from the properties
        for(auto i = properties.offset; i < properties.offset + properties.length; i++) {
            style[key_data[i].GetString()] = value_data[i].GetString();
        }

        // Convert the geometry to SVG
        return StringVector::AddString(result, GeometryToSVG(geometry, style));
	});
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsSVG(DatabaseInstance &db) {
	ScalarFunction func("ST_AsSVG", {GeoTypes::GEOMETRY(), LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)}, LogicalType::VARCHAR, GeometryAsSVGFunction, nullptr,
	                    nullptr, nullptr, GeometryFunctionLocalState::Init);
	ExtensionUtil::RegisterFunction(db, func);
}

} // namespace core

} // namespace spatial