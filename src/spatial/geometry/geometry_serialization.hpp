#pragma once

#include <cstddef>

namespace sgl {

class geometry;
struct prepared_geometry;

} // namespace sgl

namespace duckdb {

class ArenaAllocator;

// todo:
struct Serde {
	static size_t GetRequiredSize(const sgl::geometry &geom);
	static void Serialize(const sgl::geometry &geom, char *buffer, size_t buffer_size);
	static void Deserialize(sgl::geometry &result, ArenaAllocator &arena, const char *buffer, size_t buffer_size);
	static void DeserializePrepared(sgl::prepared_geometry &result, ArenaAllocator &arena, const char *buffer,
	                                size_t buffer_size);
};

} // namespace duckdb