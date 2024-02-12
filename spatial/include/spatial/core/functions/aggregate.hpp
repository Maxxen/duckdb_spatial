#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreAggregateFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStEnvelopeAgg(db);
		RegisterStSvgAgg(db);
	}

private:
	static void RegisterStEnvelopeAgg(DatabaseInstance &db);
	static void RegisterStSvgAgg(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial