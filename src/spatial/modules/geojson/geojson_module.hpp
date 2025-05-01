#pragma once

namespace duckdb {

class DatabaseInstance;

void RegisterGeoJSONModule(DatabaseInstance &db);

} // namespace duckdb