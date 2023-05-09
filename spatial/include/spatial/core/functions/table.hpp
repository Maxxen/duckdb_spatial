#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreTableFunctions {
public:
	static void Register(ClientContext &context) {
        RegisterReadGDB(context);
	}
private:
	static void RegisterReadGDB(ClientContext &context);
};

} // namespace core

} // namespace spatial