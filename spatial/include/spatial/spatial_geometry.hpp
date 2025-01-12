#pragma once

#include "core/util/math.hpp"
#include "duckdb/common/assert.hpp"
#include "geometry/geometry.hpp"

namespace spatial {

struct Serde {
	struct V1 {
		static uint32_t GetRequiredSize(const geometry::Geometry *geom, uint32_t vsize);
		static void Serialize(const geometry::Geometry *geom, uint8_t *ptr_p, uint32_t size_p);
		static geometry::Geometry Deserialize(geometry::Arena &arena, const uint8_t *ptr, uint32_t size);
	};
};

//------------------------------------------------------------------------------
// WriteCursor
//------------------------------------------------------------------------------
class WriteCursor {
public:
	WriteCursor(uint8_t *ptr_p, size_t size) : ptr(ptr_p), end(ptr_p + size) {
	}

	template <class T>
	void Write(const T &value) {
		if (ptr + sizeof(T) > end) {
			throw std::runtime_error("WriteCursor: Out of space");
		}
		memcpy(ptr, &value, sizeof(T));
		ptr += sizeof(T);
	}

	// Reserve space for `bytes` bytes and return a pointer to the reserved space, advancing the cursor
	uint8_t *Reserve(size_t bytes) {
		const auto result = ptr;
		// Check if there is enough space
		if (ptr + bytes > end) {
			throw std::runtime_error("WriteCursor: Out of space");
		}
		ptr += bytes;
		return result;
	}

	void WriteFrom(const void *src, size_t bytes) {
		if (ptr + bytes > end) {
			throw std::runtime_error("WriteCursor: Out of space");
		}
		memcpy(ptr, src, bytes);
		ptr += bytes;
	}

	void Skip(size_t bytes, bool zero) {
		if (ptr + bytes > end) {
			throw std::runtime_error("WriteCursor: Out of space");
		}
		if (zero) {
			memset(ptr, 0, bytes);
		}
		ptr += bytes;
	}

private:
	uint8_t *ptr;
	uint8_t *end;
};

//------------------------------------------------------------------------------
// ReadCursor
//------------------------------------------------------------------------------
class ReadCursor {
public:
	ReadCursor(const uint8_t *ptr_p, size_t size) : ptr(ptr_p), end(ptr_p + size) {
	}

	template <class T>
	T Read() {
		if (ptr + sizeof(T) > end) {
			throw std::runtime_error("ReadCursor: Out of space");
		}
		T value;
		memcpy(&value, ptr, sizeof(T));
		ptr += sizeof(T);
		return value;
	}

	void Skip(size_t bytes) {
		if (ptr + bytes > end) {
			throw std::runtime_error("ReadCursor: Out of space");
		}
		ptr += bytes;
	}

	void ReadInto(void *dst, size_t bytes) {
		if (ptr + bytes > end) {
			throw std::runtime_error("ReadCursor: Out of space");
		}
		memcpy(dst, ptr, bytes);
		ptr += bytes;
	}

	const uint8_t *Reserve(size_t bytes) {
		const auto result = ptr;
		// Check if there is enough space
		if (ptr + bytes > end) {
			throw std::runtime_error("ReadCursor: Out of space");
		}
		ptr += bytes;
		return result;
	}

private:
	const uint8_t *ptr;
	const uint8_t *end;
};

//---------------------------------------------------------------------------------------
// V1 Serialization
//---------------------------------------------------------------------------------------
// We always want the coordinates to be double aligned (8 bytes)
// layout:
// GeometryHeader (4 bytes)
// Padding (4 bytes) (or SRID?)
// Data (variable length)
// -- Point
// 	  Type ( 4 bytes)
//    Count (4 bytes) (count == 0 if empty point, otherwise 1)
//    X (8 bytes)
//    Y (8 bytes)
// -- LineString
//    Type (4 bytes)
//    Length (4 bytes)
//    Points (variable length)
// -- Polygon
//    Type (4 bytes)
//    NumRings (4 bytes)
// 	  RingsLengths (variable length)
//    padding (4 bytes if num_rings is odd)
//    RingsData (variable length)
// --- Multi/Point/LineString/Polygon & GeometryCollection
//    Type (4 bytes)
//    NumGeometries (4 bytes)
//    Geometries (variable length)

inline uint32_t GetRequiredSizeRecursive(const geometry::Geometry *geom, const uint32_t vsize) {

	D_ASSERT(geom->GetVertexSize() == vsize);

	switch (geom->GetType()) {
	case geometry::GeometryType::POINT:
	case geometry::GeometryType::LINESTRING: {
		// 4 bytes for the type
		// 4 bytes for the length
		// sizeof(vertex) * count;
		return 4 + 4 + geom->GetSize() * vsize;
	}
	case geometry::GeometryType::POLYGON: {
		// Polygons are special because they may pad between the rings and the ring data
		// 4 bytes for the type
		// 4 bytes for the length
		// sizeof(vertex) * count;
		uint32_t size = 4 + 4;
		for (const auto part : *geom) {
			D_ASSERT(part->IsSinglePart());

			size += 4;
			size += part->GetSize() * vsize;
		}
		if (geom->GetSize() % 2 == 1) {
			size += 4;
		}
		return size;
	}
	case geometry::GeometryType::MULTI_POINT:
	case geometry::GeometryType::MULTI_LINESTRING:
	case geometry::GeometryType::MULTI_POLYGON:
	case geometry::GeometryType::MULTI_GEOMETRY: {
		// 4 bytes for the type
		// 4 bytes for the length
		// recursive call for each part
		uint32_t size = 4 + 4;
		for (const auto part : *geom) {
			size += GetRequiredSizeRecursive(part, vsize);
		}
		return size;
	}
	default:
		D_ASSERT(false);
		return 0;
	}
}

inline uint32_t Serde::V1::GetRequiredSize(const geometry::Geometry *geom, const uint32_t vsize) {
	const auto type = geom->GetType();

	const auto has_bbox = type != geometry::GeometryType::POINT && !geom->IsEmpty();
	const auto has_z = geom->HasZ();
	const auto has_m = geom->HasM();

	const auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

	const auto head_size = 4 + 4; // type + props + padding
	const auto vert_size = dims * sizeof(double);
	const auto geom_size = GetRequiredSizeRecursive(geom, vert_size);
	const auto bbox_size = has_bbox ? dims * sizeof(float) * 2 : 0;

	const auto full_size = head_size + geom_size + bbox_size;

	// Check that the size is a multiple of 8
	D_ASSERT(full_size % 8 == 0);

	return full_size;
}

struct Vec4D {
	double x;
	double y;
	double z;
	double m;
};

struct Box4D {
	Vec4D min;
	Vec4D max;

	Box4D() : min({}), max({}) {
		min.x = min.y = min.z = min.m = NumericLimits<double>::Maximum();
		max.x = max.y = max.z = max.m = NumericLimits<double>::Minimum();
	}
};

inline void SerializeVertices(WriteCursor &cursor, const geometry::Geometry *geom, const uint32_t count,
                              const bool has_z, const bool has_m, const bool has_bbox, const uint32_t vsize,
                              Box4D &bbox) {
	const auto verts = geom->GetVertices();

	// Copy the vertices to the cursor
	const auto dst = cursor.Reserve(count * vsize);

	if (!has_bbox) {
		// Fast path, issue on memcpy to the cursor
		memcpy(dst, verts, count * vsize);
		return;
	}

	Vec4D vertex = {};
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
			bbox.min.z = std::min(bbox.min.z, vertex.z);
			bbox.max.z = std::max(bbox.max.z, vertex.z);
		}
		if (has_m) {
			bbox.min.m = std::min(bbox.min.m, vertex.m);
			bbox.max.m = std::max(bbox.max.m, vertex.m);
		}
	}
}

inline void SerializeRecursive(WriteCursor &cursor, const geometry::Geometry *geom, const bool has_z, const bool has_m,
                               const bool has_bbox, const uint32_t vsize, Box4D &bbox) {
	const auto type = geom->GetType();
	const auto count = geom->GetSize();

	cursor.Write<uint32_t>(static_cast<uint32_t>(type));
	cursor.Write<uint32_t>(count);

	switch (type) {
	case geometry::GeometryType::POINT:
	case geometry::GeometryType::LINESTRING:
		SerializeVertices(cursor, geom, count, has_z, has_m, has_bbox, vsize, bbox);
		break;
	case geometry::GeometryType::POLYGON: {
		auto ring_cursor = cursor;
		cursor.Skip((count * 4) + (count % 2 == 1 ? 4 : 0), true);
		for (const auto ring : *geom) {
			D_ASSERT(ring->IsSinglePart());
			D_ASSERT(ring->GetVertexSize() == vsize);

			const auto ring_count = ring->GetSize();
			ring_cursor.Write<uint32_t>(ring_count);
			SerializeVertices(ring_cursor, ring, ring_count, has_z, has_m, has_bbox, vsize, bbox);
		}
	} break;
	case geometry::GeometryType::MULTI_POINT:
	case geometry::GeometryType::MULTI_LINESTRING:
	case geometry::GeometryType::MULTI_POLYGON:
	case geometry::GeometryType::MULTI_GEOMETRY: {
		for (const auto part : *geom) {
			SerializeRecursive(cursor, part, has_z, has_m, has_bbox, vsize, bbox);
		}
	} break;
	default:
		D_ASSERT(false);
	}
}

inline void Serde::V1::Serialize(const geometry::Geometry *geom, uint8_t *ptr_p, uint32_t size_p) {
	const auto type = geom->GetType();

	const auto has_bbox = type != geometry::GeometryType::POINT && !geom->IsEmpty();
	const auto has_z = geom->HasZ();
	const auto has_m = geom->HasM();

	// Set flags
	uint8_t flags = 0;
	flags |= has_z ? 0x01 : 0;
	flags |= has_m ? 0x02 : 0;
	flags |= has_bbox ? 0x04 : 0;

	WriteCursor cursor(ptr_p, size_p);

	cursor.Write<uint8_t>(static_cast<uint8_t>(type));
	cursor.Write<uint8_t>(flags);
	cursor.Write<uint16_t>(0); // unused for now
	cursor.Write<uint32_t>(0); // padding

	const auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
	const auto vert_size = dims * sizeof(double);
	const auto bbox_size = has_bbox ? dims * sizeof(float) * 2 : 0;

	Box4D bbox;

	WriteCursor bbox_cursor = cursor;
	cursor.Skip(bbox_size, true);

	SerializeRecursive(cursor, geom, has_z, has_m, has_bbox, vert_size, bbox);

	if (has_bbox) {
		using core::MathUtil;
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.x)); // xmin
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.y)); // ymin
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.x));   // xmax
		bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.y));   // ymax

		if (has_z) {
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.z)); // zmin
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.z));   // zmax
		}

		if (has_m) {
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatDown(bbox.min.m)); // mmin
			bbox_cursor.Write<float>(MathUtil::DoubleToFloatUp(bbox.max.m));   // mmax
		}
	}
}

//------------------------------------------------------------------------------
// Deserialization
//------------------------------------------------------------------------------
inline void DeserializeRecursive(ReadCursor &cursor, geometry::Geometry &geom, const bool has_z, const bool has_m,
                                 geometry::Arena &arena) {
	const auto count = cursor.Read<uint32_t>();
	switch (geom.GetType()) {
	case geometry::GeometryType::POINT:
	case geometry::GeometryType::LINESTRING: {
		const auto verts = cursor.Reserve(count * geom.GetVertexSize());
		geom.SetVertices(verts, count);
	} break;
	case geometry::GeometryType::POLYGON: {
		auto ring_cursor = cursor;
		cursor.Skip((count * 4) + (count % 2 == 1 ? 4 : 0));
		for (uint32_t i = 0; i < count; i++) {
			const auto ring_count = ring_cursor.Read<uint32_t>();
			const auto verts = cursor.Reserve(ring_count * geom.GetVertexSize());
			const auto ring = arena.Make<geometry::Geometry>(geometry::GeometryType::LINESTRING, has_z, has_m);
			ring->SetVertices(verts, ring_count);
			geom.AppendPart(ring);
		}
	} break;
	case geometry::GeometryType::MULTI_POINT:
	case geometry::GeometryType::MULTI_LINESTRING:
	case geometry::GeometryType::MULTI_POLYGON:
	case geometry::GeometryType::MULTI_GEOMETRY: {
		for (uint32_t i = 0; i < count; i++) {
			const auto part_type = static_cast<geometry::GeometryType>(cursor.Read<uint32_t>());
			const auto part = arena.Make<geometry::Geometry>(part_type, has_z, has_m);
			DeserializeRecursive(cursor, *part, has_z, has_m, arena);
			geom.AppendPart(part);
		}
	} break;
	default:
		break;
	}
}

inline geometry::Geometry Serde::V1::Deserialize(geometry::Arena &arena, const uint8_t *ptr, const uint32_t size) {

	ReadCursor cursor(ptr, size);

	const auto type = static_cast<geometry::GeometryType>(cursor.Read<uint8_t>());
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

	if(has_bbox) {
		// Skip past bbox if present
		cursor.Skip(sizeof(float) * 2 * (2 + has_z + has_m));
	}

	// Create root geometry
	geometry::Geometry root(type, has_z, has_m);

	// Deserialize the geometry
	DeserializeRecursive(cursor, root, has_z, has_m, arena);

	return root;
}

} // namespace spatial
