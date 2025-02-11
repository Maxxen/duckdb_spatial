#pragma once

namespace duckdb {

static constexpr const uint8_t GEOMETRY_VERSION = 0;

struct GeometryProperties {
private:
	static constexpr const uint8_t Z = 0x01;
	static constexpr const uint8_t M = 0x02;
	static constexpr const uint8_t BBOX = 0x04;
	// Example of other useful properties:
	// static constexpr const uint8_t EMPTY = 0x08;
	// static constexpr const uint8_t GEODETIC = 0x10;
	// static constexpr const uint8_t SOLID = 0x20;
	static constexpr const uint8_t VERSION_1 = 0x40;
	static constexpr const uint8_t VERSION_0 = 0x80;
	uint8_t flags = 0;

public:
	explicit GeometryProperties(uint8_t flags = 0) : flags(flags) {
	}
	GeometryProperties(bool has_z, bool has_m) {
		SetZ(has_z);
		SetM(has_m);
	}

	inline void CheckVersion() const {
		const auto v0 = (flags & VERSION_0);
		const auto v1 = (flags & VERSION_1);
		if ((v1 | v0) != GEOMETRY_VERSION) {
			throw NotImplementedException(
			    "This geometry seems to be written with a newer version of the DuckDB spatial library that is not "
			    "compatible with this version. Please upgrade your DuckDB installation.");
		}
	}

	inline bool HasZ() const {
		return (flags & Z) != 0;
	}
	inline bool HasM() const {
		return (flags & M) != 0;
	}
	inline bool HasBBox() const {
		return (flags & BBOX) != 0;
	}
	inline void SetZ(bool value) {
		flags = value ? (flags | Z) : (flags & ~Z);
	}
	inline void SetM(bool value) {
		flags = value ? (flags | M) : (flags & ~M);
	}
	inline void SetBBox(bool value) {
		flags = value ? (flags | BBOX) : (flags & ~BBOX);
	}

	uint32_t VertexSize() const {
		return sizeof(double) * (2 + HasZ() + HasM());
	}
};

} // namespace duckdb