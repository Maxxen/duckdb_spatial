#define DUCKDB_EXTENSION_MAIN

#include "spatial/spatial_extension.hpp"

#include "duckdb.hpp"
#include "index/rtree/rtree.hpp"
#include "spatial/index/rtree/rtree_module.hpp"
#include "spatial/modules/gdal/gdal_module.hpp"
#include "spatial/modules/geos/geos_module.hpp"
#include "spatial/modules/main/spatial_functions.hpp"
#include "spatial/modules/osm/osm_module.hpp"
#include "spatial/modules/proj/proj_module.hpp"
#include "spatial/modules/shapefile/shapefile_module.hpp"
#include "spatial/spatial_optimizers.hpp"
#include "spatial/spatial_types.hpp"
#include "spatial/spatial_geoarrow.hpp"
#include "spatial/modules/fgb/fgb_module.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {

	// Register the types
	GeoTypes::Register(instance);

	RegisterSpatialCastFunctions(instance);
	RegisterSpatialScalarFunctions(instance);
	RegisterSpatialAggregateFunctions(instance);
	RegisterSpatialTableFunctions(instance);
	RegisterSpatialOptimizers(instance);
	GeoArrow::Register(instance);

	RegisterProjModule(instance);
	RegisterGDALModule(instance);
	RegisterGEOSModule(instance);
	RegisterOSMModule(instance);
	RegisterShapefileModule(instance);

	RegisterReadFGB(instance);

	RTreeModule::RegisterIndex(instance);
	RTreeModule::RegisterIndexPragmas(instance);
	RTreeModule::RegisterIndexScan(instance);
	RTreeModule::RegisterIndexPlanCreate(instance);
	RTreeModule::RegisterIndexPlanScan(instance);
}

void SpatialExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string SpatialExtension::Name() {
	return "spatial";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void spatial_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *spatial_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
