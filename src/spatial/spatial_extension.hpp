#pragma once

#include "duckdb.hpp"

namespace duckdb {

class SpatialExtension final : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
