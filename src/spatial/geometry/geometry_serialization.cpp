#include "spatial/geometry/geometry_serialization.hpp"
#include "spatial/util/binary_reader.hpp"
#include "spatial/util/binary_writer.hpp"
#include "spatial/util/math.hpp"
#include "spatial/geometry/sgl.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

// TODO: Make non-recursive

static size_t GetRequiredSizeInternal(const sgl::geometry *geom) {
	const auto vertex_size = geom->get_vertex_size();
	const auto part_count = geom->get_count();

	switch (geom->get_type()) {
	case sgl::geometry_type::POINT:
	case sgl::geometry_type::LINESTRING:
		// 4 bytes for the type
		// 4 bytes for the length
		// sizeof(vertex) * count;
		return 4 + 4 + part_count * vertex_size;
	case sgl::geometry_type::POLYGON: {
		// Polygons are special because they may pad between the rings and the ring data
		// 4 bytes for the type
		// 4 bytes for the length
		// sizeof(vertex) * count;
		size_t size = 4 + 4;

		const auto tail = geom->get_last_part();
		if (!tail) {
			return size;
		}
		auto part = tail;
		do {
			part = part->get_next();
			size += 4 + part->get_count() * vertex_size;
		} while (part != tail);

		if (part_count % 2 == 1) {
			size += 4;
		}
		return size;
	}
	case sgl::geometry_type::MULTI_POINT:
	case sgl::geometry_type::MULTI_LINESTRING:
	case sgl::geometry_type::MULTI_POLYGON:
	case sgl::geometry_type::MULTI_GEOMETRY: {
		// 4 bytes for the type
		// 4 bytes for the length
		// recursive call for each part
		size_t size = 4 + 4;
		const auto tail = geom->get_last_part();
		if (!tail) {
			return size;
		}
		auto part = tail;
		do {
			part = part->get_next();
			size += GetRequiredSizeInternal(part);
		} while (part != tail);
		return size;
	}
	default:
		D_ASSERT(false);
		return 0;
	}
}

size_t Serde::GetRequiredSize(const sgl::geometry &geom) {
	const auto type = geom.get_type();

	const auto has_bbox = type != sgl::geometry_type::POINT && !geom.is_empty();
	const auto has_z = geom.has_z();
	const auto has_m = geom.has_m();

	const auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

	const auto head_size = 4 + 4; // type + props + padding
	const auto geom_size = GetRequiredSizeInternal(&geom);
	const auto bbox_size = has_bbox ? dims * sizeof(float) * 2 : 0;

	const auto full_size = head_size + geom_size + bbox_size;

	// Check that the size is a multiple of 8
	D_ASSERT(full_size % 8 == 0);

	return full_size;
}

static void SerializeVertices(BinaryWriter &cursor, const sgl::geometry *geom, const uint32_t count, const bool has_z,
                              const bool has_m, const bool has_bbox, const uint32_t vsize, sgl::box_xyzm &bbox) {

	const auto verts = geom->get_vertex_data();

	// Copy the vertices to the cursor
	const auto dst = cursor.Reserve(count * vsize);

	if (!has_bbox) {
		// Fast path, issue on memcpy to the cursor
		memcpy(dst, verts, count * vsize);
		return;
	}

	sgl::vertex_xyzm vertex = {0};
	for (uint32_t i = 0; i < count; i++) {

		// Load the vertex from the geometry
		memcpy(&vertex, verts + i * vsize, vsize);

		// Copy the vertex to the cursor
		memcpy(dst + i * vsize, &vertex, vsize);

		bbox.min.x = std::min(bbox.min.x, vertex.x);
		bbox.min.y = std::min(bbox.min.y, vertex.y);
		bbox.max.x = std::max(bbox.max.x, vertex.x);
		bbox.max.y = std::max(bbox.max.y, vertex.y);

		if (has_z) {
			bbox.min.zm = std::min(bbox.min.zm, vertex.zm);
			bbox.max.zm = std::max(bbox.max.zm, vertex.zm);
		}
		if (has_m) {
			bbox.min.m = std::min(bbox.min.m, vertex.m);
			bbox.max.m = std::max(bbox.max.m, vertex.m);
		}
	}
}

static void SerializeRecursive(BinaryWriter &cursor, const sgl::geometry *geom, const bool has_z, const bool has_m,
                               const bool has_bbox, const uint32_t vsize, sgl::box_xyzm &bbox) {
	const auto type = geom->get_type();
	const auto count = geom->get_count();

	if (type < sgl::geometry_type::POINT || type > sgl::geometry_type::MULTI_GEOMETRY) {
		throw InvalidInputException("Cannot serialize geometry of type %d", static_cast<int>(type));
	}

	// The GeometryType enum used to start with POINT = 0
	// but now it starts with INVALID = 0, so we need to subtract 1
	cursor.Write<uint32_t>(static_cast<uint32_t>(type) - 1);
	cursor.Write<uint32_t>(count);

	switch (type) {
	case sgl::geometry_type::POINT:
	case sgl::geometry_type::LINESTRING:
		SerializeVertices(cursor, geom, count, has_z, has_m, has_bbox, vsize, bbox);
		break;
	case sgl::geometry_type::POLYGON: {
		auto ring_cursor = cursor;
		cursor.Skip((count * 4) + (count % 2 == 1 ? 4 : 0), true);

		const auto tail = geom->get_last_part();
		if (!tail) {
			break;
		}

		auto ring = tail;
		do {
			ring = ring->get_next();
			ring_cursor.Write<uint32_t>(ring->get_count());
			SerializeVertices(cursor, ring, ring->get_count(), has_z, has_m, has_bbox, vsize, bbox);
		} while (ring != tail);

	} break;
	case sgl::geometry_type::MULTI_POINT:
	case sgl::geometry_type::MULTI_LINESTRING:
	case sgl::geometry_type::MULTI_POLYGON:
	case sgl::geometry_type::MULTI_GEOMETRY: {
		const auto tail = geom->get_last_part();
		if (!tail) {
			break;
		}

		auto part = tail;
		do {
			part = part->get_next();
			SerializeRecursive(cursor, part, has_z, has_m, has_bbox, vsize, bbox);
		} while (part != tail);
	} break;
	default:
		D_ASSERT(false);
	}
}

void Serde::Serialize(const sgl::geometry &geom, char *buffer, size_t buffer_size) {
	const auto type = geom.get_type();

	const auto has_bbox = type != sgl::geometry_type::POINT && !geom.is_empty();
	const auto has_z = geom.has_z();
	const auto has_m = geom.has_m();

	// Set flags
	uint8_t flags = 0;
	flags |= has_z ? 0x01 : 0;
	flags |= has_m ? 0x02 : 0;
	flags |= has_bbox ? 0x04 : 0;

	BinaryWriter cursor(buffer, buffer_size);

	if (type == sgl::geometry_type::INVALID) {
		throw InvalidInputException("Cannot serialize geometry of type INVALID");
	}

	// The GeometryType enum used to start with POINT = 0
	// but now it starts with INVALID = 0, so we need to subtract 1
	cursor.Write<uint8_t>(static_cast<uint8_t>(type) - 1);
	cursor.Write<uint8_t>(flags);
	cursor.Write<uint16_t>(0); // unused for now
	cursor.Write<uint32_t>(0); // padding

	const auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
	const auto vert_size = dims * sizeof(double);
	const auto bbox_size = has_bbox ? dims * sizeof(float) * 2 : 0;

	// Setup a bbox to store the min/max values
	sgl::box_xyzm bbox = sgl::box_xyzm::smallest();

	auto bbox_cursor = cursor;
	cursor.Skip(bbox_size, true);

	SerializeRecursive(cursor, &geom, has_z, has_m, has_bbox, vert_size, bbox);

	if (has_bbox) {
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.x)); // xmin
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.y)); // ymin
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.x));   // xmax
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.y));   // ymax

		if (has_z) {
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.zm)); // zmin
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.zm));   // zmax
		}

		if (has_m) {
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.m)); // mmin
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.m));   // mmax
		}
	}
}

static void DeserializeRecursive(BinaryReader &cursor, sgl::geometry &geom, const bool has_z, const bool has_m,
                                 ArenaAllocator &arena) {
	const auto count = cursor.Read<uint32_t>();
	switch (geom.get_type()) {
	case sgl::geometry_type::POINT:
	case sgl::geometry_type::LINESTRING: {
		const auto verts = cursor.Reserve(count * geom.get_vertex_size());
		geom.set_vertex_data(verts, count);
	} break;
	case sgl::geometry_type::POLYGON: {
		auto ring_cursor = cursor;
		cursor.Skip((count * 4) + (count % 2 == 1 ? 4 : 0));
		for (uint32_t i = 0; i < count; i++) {
			const auto ring_count = ring_cursor.Read<uint32_t>();
			const auto verts = cursor.Reserve(ring_count * geom.get_vertex_size());

			auto ring_mem = arena.AllocateAligned(sizeof(sgl::geometry));
			const auto ring = new (ring_mem) sgl::geometry(sgl::geometry_type::LINESTRING);

			ring->set_z(has_z);
			ring->set_m(has_m);
			ring->set_vertex_data(verts, ring_count);

			geom.append_part(ring);
		}
	} break;
	case sgl::geometry_type::MULTI_POINT:
	case sgl::geometry_type::MULTI_LINESTRING:
	case sgl::geometry_type::MULTI_POLYGON:
	case sgl::geometry_type::MULTI_GEOMETRY: {
		for (uint32_t i = 0; i < count; i++) {
			const auto part_type = static_cast<sgl::geometry_type>(cursor.Read<uint32_t>() + 1);
			auto part_mem = arena.AllocateAligned(sizeof(sgl::geometry));
			const auto part = new (part_mem) sgl::geometry(part_type);
			part->set_z(has_z);
			part->set_m(has_m);
			DeserializeRecursive(cursor, *part, has_z, has_m, arena);

			geom.append_part(part);
		}
	} break;
	default:
		break;
	}
}

void Serde::Deserialize(sgl::geometry &result, ArenaAllocator &arena, const char *buffer, size_t buffer_size) {

	BinaryReader cursor(buffer, buffer_size);

	const auto type = static_cast<sgl::geometry_type>(cursor.Read<uint8_t>() + 1);
	const auto flags = cursor.Read<uint8_t>();
	cursor.Skip(sizeof(uint16_t));
	cursor.Skip(sizeof(uint32_t)); // padding

	// Parse flags
	const auto has_z = (flags & 0x01) != 0;
	const auto has_m = (flags & 0x02) != 0;
	const auto has_bbox = (flags & 0x04) != 0;

	const auto format_v1 = (flags & 0x40) != 0;
	const auto format_v0 = (flags & 0x80) != 0;

	if (format_v1 || format_v0) {
		// Unsupported version, throw an error
		throw NotImplementedException(
		    "This geometry seems to be written with a newer version of the DuckDB spatial library that is not "
		    "compatible with this version. Please upgrade your DuckDB installation.");
	}

	if (has_bbox) {
		// Skip past bbox if present
		cursor.Skip(sizeof(float) * 2 * (2 + has_z + has_m));
	}

	// Create root geometry
	result.set_type(type);
	result.set_z(has_z);
	result.set_m(has_m);

	// Read the first type
	cursor.Read<uint32_t>();

	// Deserialize the geometry
	DeserializeRecursive(cursor, result, has_z, has_m, arena);
}

} // namespace duckdb