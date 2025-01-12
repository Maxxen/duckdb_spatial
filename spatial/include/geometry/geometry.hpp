#pragma once

#include <cstdint>
#include <type_traits>

// include assertions if not already defined
#ifndef D_ASSERT
#include <cassert>
#define D_ASSERT(x) assert(x)
#endif

namespace geometry {

//-------------------------------------------------------------------------
// Arena
//-------------------------------------------------------------------------
class Arena {
public:
	virtual ~Arena() = default;
	virtual void *Allocate(size_t size) = 0;
	virtual void *Reallocate(void *ptr, size_t old_size, size_t new_size) = 0;
	virtual void Reset() = 0;

	template <class TYPE, class... ARGS>
	TYPE *Make(ARGS &&...args) {
		static_assert(std::is_trivially_destructible<TYPE>::value,
		              "TYPE must be trivial to be able to use Arena::Make()!");
		auto ptr = Allocate(sizeof(TYPE));
		return new (ptr) TYPE(std::forward<ARGS>(args)...);
	}
};

//-------------------------------------------------------------------------
// Geometry
//-------------------------------------------------------------------------

enum class GeometryType : uint8_t {
	POINT = 0,
	LINESTRING = 1,
	POLYGON = 2,
	MULTI_POINT = 3,
	MULTI_LINESTRING = 4,
	MULTI_POLYGON = 5,
	MULTI_GEOMETRY = 6,
};

class GeometryPartIterator;
class ConstGeometryPartIterator;

class Geometry {
	friend class GeometryPartIterator;
	friend class ConstGeometryPartIterator;

public:
	Geometry()
	    : type(GeometryType::POINT), flag(0), pad1(0), pad2(0), size(0), data(nullptr), next(nullptr), xptr(nullptr) {
	}

	explicit Geometry(const GeometryType type_p, const bool has_z = false, const bool has_m = false)
	    : type(type_p), flag(0), pad1(0), pad2(0), size(0), data(nullptr), next(nullptr), xptr(nullptr) {
		SetZ(has_z);
		SetM(has_m);
	}

public:
	GeometryType GetType() const;
	void SetTypeUnsafe(GeometryType type_p);
	uint32_t GetSize() const;

	bool IsEmpty() const;
	bool HasZ() const;
	bool HasM() const;
	void SetZ(bool value);
	void SetM(bool value);
	uint32_t GetVertexSize() const;

	bool IsMultiPart() const;
	bool IsSinglePart() const;
	bool IsCollection() const;

	const uint8_t *GetVertices() const;
	uint8_t *GetVertices();
	void SetVertices(const uint8_t *ptr_p, uint32_t size_p);

	Geometry *GetFirstPart();
	Geometry *GetLastPart();
	Geometry *GetNextPart();

	const Geometry *GetFirstPart() const;
	const Geometry *GetLastPart() const;
	const Geometry *GetNextPart() const;
	// O(N) operation, index wraps around
	Geometry *GetPartAt(uint32_t index);
	const Geometry *GetPartAt(uint32_t index) const;

	void AppendPart(Geometry *part);

	// Iterator helpers
	GeometryPartIterator begin();
	GeometryPartIterator end();
	ConstGeometryPartIterator begin() const;
	ConstGeometryPartIterator end() const;

private:
	GeometryType type;
	uint8_t flag;
	uint8_t pad1;
	uint8_t pad2;
	uint32_t size;

	void *data;
	Geometry *next;
	void *xptr;
};

class GeometryPartIterator {
	friend class Geometry;

public:
	Geometry *operator*() const {
		return curr;
	}
	bool operator!=(const GeometryPartIterator &other) const {
		return curr != other.curr && prev != other.prev;
	}
	GeometryPartIterator &operator++() {
		prev = curr;
		curr = curr->next;
		return *this;
	}

private:
	GeometryPartIterator(Geometry *prev_p, Geometry *curr_p) : prev(prev_p), curr(curr_p) {
	}

	Geometry *prev;
	Geometry *curr;
};

class ConstGeometryPartIterator {
	friend class Geometry;

public:
	const Geometry *operator*() const {
		return curr;
	}
	bool operator!=(const ConstGeometryPartIterator &other) const {
		return curr != other.curr && prev != other.prev;
	}
	ConstGeometryPartIterator &operator++() {
		prev = curr;
		curr = curr->next;
		return *this;
	}

private:
	ConstGeometryPartIterator(const Geometry *prev_p, const Geometry *curr_p) : prev(prev_p), curr(curr_p) {
	}

	const Geometry *prev;
	const Geometry *curr;
};

//-------------------------------------------------------------------------
// Inlined methods
//-------------------------------------------------------------------------

inline GeometryType Geometry::GetType() const {
    return type;
}

inline void Geometry::SetTypeUnsafe(const GeometryType type_p) {
	type = type_p;
}


inline uint32_t Geometry::GetSize() const {
	return size;
}
inline bool Geometry::IsEmpty() const {
	return size == 0;
}
inline bool Geometry::HasZ() const {
	return flag & 0x01;
}
inline bool Geometry::HasM() const {
	return flag & 0x02;
}
inline void Geometry::SetZ(const bool value) {
	flag = value ? flag | 0x01 : flag & ~0x01;
}
inline void Geometry::SetM(const bool value) {
	flag = value ? flag | 0x02 : flag & ~0x02;
}
inline uint32_t Geometry::GetVertexSize() const {
	return (2 + (HasZ() ? 1 : 0) + (HasM() ? 1 : 0)) * sizeof(double);
}

inline bool Geometry::IsMultiPart() const {
	return type >= GeometryType::POLYGON && type <= GeometryType::MULTI_GEOMETRY;
}

inline bool Geometry::IsSinglePart() const {
	return type >= GeometryType::POINT && type <= GeometryType::LINESTRING;
}

inline bool Geometry::IsCollection() const {
	return type >= GeometryType::MULTI_POINT && type <= GeometryType::MULTI_GEOMETRY;
}

inline const uint8_t *Geometry::GetVertices() const {
	D_ASSERT(IsSinglePart());
	return static_cast<const uint8_t *>(data);
}

inline uint8_t *Geometry::GetVertices() {
	D_ASSERT(IsSinglePart());
	return static_cast<uint8_t *>(data);
}

inline void Geometry::SetVertices(const uint8_t *ptr_p, const uint32_t size_p) {
	D_ASSERT(IsSinglePart());
	data = const_cast<uint8_t *>(ptr_p);
	size = size_p;
}

inline Geometry *Geometry::GetFirstPart() {
	const auto tail = GetLastPart();
	return tail ? tail->next : nullptr;
}

inline Geometry *Geometry::GetLastPart() {
	D_ASSERT(IsMultiPart());
	return static_cast<Geometry *>(data);
}

inline Geometry *Geometry::GetNextPart() {
	return next;
}

inline const Geometry *Geometry::GetFirstPart() const {
	const auto tail = GetLastPart();
	return tail ? tail->next : nullptr;
}

inline const Geometry *Geometry::GetLastPart() const {
	D_ASSERT(IsMultiPart());
	return static_cast<const Geometry *>(data);
}

inline const Geometry *Geometry::GetNextPart() const {
	return next;
}

inline Geometry *Geometry::GetPartAt(const uint32_t index) {
	auto part = GetFirstPart();
	if (!part) {
		return nullptr;
	}
	for (uint32_t i = 0; i < index; i++) {
		D_ASSERT(part); // parts should always form a circular linked list, no gaps!
		part = part->next;
	}
	return part;
}

inline const Geometry *Geometry::GetPartAt(const uint32_t index) const {
	auto part = GetFirstPart();
	if (!part) {
		return nullptr;
	}
	for (uint32_t i = 0; i < index; i++) {
		D_ASSERT(part); // parts should always form a circular linked list, no gaps!
		part = part->next;
	}
	return part;
}

inline void Geometry::AppendPart(Geometry *part) {
	const auto tail = GetLastPart();
	if (!tail) {
		data = part;
		part->next = part;
		size = 1;
	} else {
		data = part;
		const auto head = tail->next;
		tail->next = part;
		part->next = head;
		size++;
	}
}

inline GeometryPartIterator Geometry::begin() {
	return {nullptr, GetFirstPart()};
}

inline GeometryPartIterator Geometry::end() {
	return {GetLastPart(), GetFirstPart()};
}

inline ConstGeometryPartIterator Geometry::begin() const {
	return {nullptr, GetFirstPart()};
}

inline ConstGeometryPartIterator Geometry::end() const {
	return {GetLastPart(), GetFirstPart()};
}

//-------------------------------------------------------------------------
// Operations
//-------------------------------------------------------------------------

namespace operations {
	double LinestringGetSignedArea(const Geometry &geom);
	double GeometryGetArea(const Geometry &geom);
	double PolygonGetArea(const Geometry &geom);
	double MultiPolygonGetArea(const Geometry &geom);

	uint32_t GeometryGetNumVertices(const Geometry &geom);

	// Returns 0 for empty geometries
	int32_t GeometryGetDimension(const Geometry &geom);

	Geometry GetPointFromLine(const Geometry &geom, uint32_t index);

}


} // namespace geometry