#include "duckdb/common/types/null_value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/aggregate.hpp"
#include "duckdb/function/scalar_macro_function.hpp"

#include "duckdb/catalog/default/default_functions.hpp"

namespace spatial {

namespace core {

void CoreAggregateFunctions::RegisterStSvgAgg(DatabaseInstance &db) {

    DefaultMacro macro = {nullptr};
    macro.schema = DEFAULT_SCHEMA;
    macro.name = "st_svg_agg";
    macro.parameters[0] = "geom";
    macro.parameters[1] = "style";
    macro.macro = R"--(
        format('<svg viewBox="{} {} {} {}" width="{}" height="{}" xmlns="http://www.w3.org/2000/svg"> {} </svg>',
        st_xmin(st_envelope_agg(geom)) - (st_xmax(st_envelope_agg(geom)) - st_xmin(st_envelope_agg(geom))) * padding,
        st_ymin(st_envelope_agg(geom)) - (st_ymax(st_envelope_agg(geom)) - st_ymin(st_envelope_agg(geom))) * padding,
        (st_xmax(st_envelope_agg(geom)) - st_xmin(st_envelope_agg(geom))) + (st_xmax(st_envelope_agg(geom)) - st_xmin(st_envelope_agg(geom))) * padding,
        (st_xmax(st_envelope_agg(geom)) - st_xmin(st_envelope_agg(geom))) + (st_ymax(st_envelope_agg(geom)) - st_ymin(st_envelope_agg(geom))) * padding,
        width,
        height,
        string_agg(ST_AsSVG(geom, style), ''));
    )--";

    auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(macro);
    info->function->default_parameters["order_by"] = make_uniq<ConstantExpression>(Value::BIGINT(0));
    info->function->default_parameters["padding"] = make_uniq<ConstantExpression>(Value::DOUBLE(0.1));
    info->function->default_parameters["width"] = make_uniq<ConstantExpression>(Value::BIGINT(100));
    info->function->default_parameters["height"] = make_uniq<ConstantExpression>(Value::BIGINT(100));
    info->parameter_names = {"geom", "style", "order_by", "padding", "width", "height"};
    ExtensionUtil::RegisterFunction(db, *info);

    // ExtensionUtil::RegisterFunction(db, string_agg);
}

} // namespace core

} // namespace spatial
