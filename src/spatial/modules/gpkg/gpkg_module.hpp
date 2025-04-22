#pragma once

namespace duckdb {

class DatabaseInstance;

class GPKGModule {
public:
	static void Register(DatabaseInstance &db);
};

} // namespace duckdb