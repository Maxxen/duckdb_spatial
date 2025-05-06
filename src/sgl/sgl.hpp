#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <limits>
#include <cmath>

// Assert macro
#ifndef SGL_ASSERT
#ifdef NDEBUG
#define SGL_ASSERT(x) ((void)0)
#else
#include <cassert>
#define SGL_ASSERT(x) assert(x)
#endif
#endif

namespace sgl {

struct allocator {
	virtual void *alloc(size_t size) = 0;
	virtual void dealloc(void *ptr, size_t size) = 0;
	virtual void *realloc(void *ptr, size_t old_size, size_t new_size) = 0;
	virtual ~allocator() = default;
};

struct vertex_xy {
	double x;
	double y;

	bool operator==(const vertex_xy &other) const {
		return x == other.x && y == other.y;
	}

	vertex_xy operator-(const vertex_xy &other) const {
		return {x - other.x, y - other.y};
	}

	vertex_xy operator+(const vertex_xy &other) const {
		return {x + other.x, y + other.y};
	}

	vertex_xy operator*(double scalar) const {
		return {x * scalar, y * scalar};
	}

	vertex_xy operator/(double scalar) const {
		return {x / scalar, y / scalar};
	}
};

struct vertex_xyzm {
	double x;
	double y;
	double zm;
	double m;

	bool operator==(const vertex_xyzm &other) const {
		return x == other.x && y == other.y && zm == other.zm && m == other.m;
	}

	vertex_xyzm operator-(const vertex_xyzm &other) const {
		return {x - other.x, y - other.y, zm - other.zm, m - other.m};
	}

	vertex_xyzm operator+(const vertex_xyzm &other) const {
		return {x + other.x, y + other.y, zm + other.zm, m + other.m};
	}

	vertex_xyzm operator*(double scalar) const {
		return {x * scalar, y * scalar, zm * scalar, m * scalar};
	}

	vertex_xyzm operator/(double scalar) const {
		return {x / scalar, y / scalar, zm / scalar, m / scalar};
	}
};

struct box_xy {
	vertex_xy min;
	vertex_xy max;

	static box_xy smallest() {
		constexpr auto dmax = std::numeric_limits<double>::max();
		constexpr auto dmin = std::numeric_limits<double>::lowest();
		return {
		    {dmax, dmax},
		    {dmin, dmin},
		};
	}

	bool intersects(const box_xy &other) const {
		return !(min.x > other.max.x || max.x < other.min.x || min.y > other.max.y || max.y < other.min.y);
	}

	bool contains(const vertex_xy &other) const {
		return (min.x <= other.x && max.x >= other.x && min.y <= other.y && max.y >= other.y);
	}

	double distance_to(const vertex_xy &other) const {
		if (contains(other)) {
			return 0.0;
		}

		const auto dx = std::max(min.x - other.x, other.x - max.x);
		const auto dy = std::max(min.y - other.y, other.y - max.y);
		return std::hypot(dx, dy);
	}
};

struct box_xyzm {
	vertex_xyzm min;
	vertex_xyzm max;

	static box_xyzm smallest() {
		constexpr auto dmax = std::numeric_limits<double>::max();
		constexpr auto dmin = std::numeric_limits<double>::lowest();
		return {
		    {dmax, dmax, dmax, dmax},
		    {dmin, dmin, dmin, dmin},
		};
	}
};

struct affine_matrix {

	// *------------*
	// | a b c xoff |
	// | d e f yoff |
	// | g h i zoff |
	// | 0 0 0 1	|
	// *------------*
	double v[16] = {};

	static affine_matrix identity() {
        affine_matrix result;
        result.v[0] = 1;
        result.v[5] = 1;
        result.v[10] = 1;
        return result;
    }

	static affine_matrix translate(double x, double y, double z = 0) {
        affine_matrix result = identity();
        result.v[3] = x;
        result.v[7] = y;
        result.v[11] = z;
        return result;
    }

	static affine_matrix scale(double x, double y, double z = 1) {
        affine_matrix result = identity();
        result.v[0] = x;
        result.v[5] = y;
        result.v[10] = z;
        return result;
    }

	static affine_matrix rotate_x(double angle) {
        affine_matrix result = identity();
        const auto c = std::cos(angle);
        const auto s = std::sin(angle);
        result.v[5] = c;
        result.v[6] = -s;
        result.v[9] = s;
        result.v[10] = c;
        return result;
    }

	static affine_matrix rotate_y(double angle) {
		affine_matrix result = identity();
		const auto c = std::cos(angle);
		const auto s = std::sin(angle);
		result.v[0] = c;
		result.v[2] = s;
		result.v[8] = -s;
		result.v[10] = c;
		return result;
	}

	static affine_matrix rotate_z(double angle) {
		affine_matrix result = identity();
		const auto c = std::cos(angle);
		const auto s = std::sin(angle);
		result.v[0] = c;
		result.v[1] = -s;
		result.v[4] = s;
		result.v[5] = c;
		return result;
	}

	static affine_matrix translate_scale(double x, double y, double z, double sx, double sy, double sz) {
        affine_matrix result = identity();
        result.v[0] = sx;
        result.v[5] = sy;
        result.v[10] = sz;
        result.v[3] = x;
        result.v[7] = y;
        result.v[11] = z;
        return result;
    }

	vertex_xy apply_xy(const vertex_xy &vertex) const {

		// x = a * x + b * y + xoff;
		// y = d * x + e * y + yoff;

		vertex_xy result = {0, 0};
		result.x = v[0] * vertex.x + v[1] * vertex.y + v[3];
		result.y = v[4] * vertex.x + v[5] * vertex.y + v[7];
		return result;
	}

	vertex_xyzm apply_xyz(const vertex_xyzm &vertex) const {

		// x = a * x + b * y + c * z + xoff;
		// y = d * x + e * y + f * z + yoff;
		// z = g * x + h * y + i * z + zoff;

		vertex_xyzm result = {0, 0, 0, 0};
		result.x =  v[0] * vertex.x + v[1] * vertex.y + v[2]  * vertex.zm + v[3];
		result.y =  v[4] * vertex.x + v[5] * vertex.y + v[6]  * vertex.zm + v[7];
		result.zm = v[8] * vertex.x + v[9] * vertex.y + v[10] * vertex.zm + v[11];
        return result;
    }
};

enum class geometry_type : uint8_t {
	INVALID = 0,
	POINT,
	LINESTRING,
	POLYGON,
	MULTI_POINT,
	MULTI_LINESTRING,
	MULTI_POLYGON,
	MULTI_GEOMETRY,
};

enum class vertex_type : uint8_t {
	XY = 0,
	XYZ = 1,
	XYM = 2,
	XYZM = 3,
};

class geometry {
private:
	// clang-format off
	geometry_type	type = geometry_type::INVALID;
	uint8_t			flag = 0;
	uint16_t		padd = 0;
	uint32_t		size = 0;
	void*			data = nullptr;
	geometry*		next = nullptr;
	geometry*		prnt = nullptr;
	// clang-format on
public:
	geometry() = default;
	explicit geometry(const geometry_type type, const bool has_z = false, const bool has_m = false) : type(type) {
		set_z(has_z);
		set_m(has_m);
	}

	// Not copyable
	// Avoid accidental copies, there are may be parent/child relationships pointing to this object
	geometry(const geometry &) = delete;
	geometry &operator=(const geometry &) = delete;

	// Not movable
	// there might be parent/child relationships pointing to this object
	geometry(geometry &&other) = delete;
	geometry &operator=(geometry &&other) = delete;

	geometry_type get_type() const;
	void set_type(geometry_type type);

	bool has_z() const;
	bool has_m() const;
	bool is_prepared() const;
	bool set_z(bool value);
	bool set_m(bool value);
	bool set_prepared(bool value);

	bool is_single_part() const;
	bool is_multi_part() const;
	bool is_collection() const;

	uint32_t get_count() const;
	void set_count(uint32_t count);
	bool is_empty() const;

	const geometry *get_last_part() const;
	const geometry *get_first_part() const;
	const geometry *get_nth_part(uint32_t n) const;
	const geometry *get_next() const;
	const geometry *get_parent() const;

	geometry *get_last_part();
	geometry *get_first_part();
	geometry *get_nth_part(uint32_t n);
	geometry *get_next();
	geometry *get_parent();

	void append_part(geometry *part);

	typedef bool (*select_func)(void *state, const geometry *part);
	typedef void (*handle_func)(void *state, geometry *part);

	void filter_parts(void *state, select_func select, handle_func handle);

	// removes the first part and returns it. Returns nullptr if there are no parts.
	geometry *pop_first_part();

	const uint8_t *get_vertex_data() const;
	uint8_t *get_vertex_data();
	void set_vertex_data(const uint8_t *data, uint32_t size);
	void set_vertex_data(const char *data, uint32_t size);

	void allocate_vertex_data(allocator *alloc, uint32_t size);
	// Requires that the vertex data has been allocated with allocate_vertex_data
	void realloc_vertex_data(allocator *alloc, uint32_t new_size);

	size_t get_vertex_size() const;
	vertex_xy get_vertex_xy(uint32_t n) const;
	vertex_xyzm get_vertex_xyzm(uint32_t n) const;

	void set_vertex_xy(uint32_t n, const vertex_xy &vertex);

	static std::string type_to_string(geometry_type type);
};


} // namespace sgl

//--------------------------------------------------------------------------
// Implementation
//--------------------------------------------------------------------------

namespace sgl {

inline geometry_type geometry::get_type() const {
	return type;
}

inline void geometry::set_type(const geometry_type type) {
	this->type = type;
}

inline bool geometry::has_z() const {
	return flag & 0x01;
}

inline bool geometry::has_m() const {
	return flag & 0x02;
}

inline bool geometry::is_prepared() const {
	return flag & 0x04;
}

inline bool geometry::set_prepared(const bool value) {
	if (value) {
		flag |= 0x04;
	} else {
		flag &= ~0x04;
	}
	return value;
}

inline bool geometry::set_z(const bool value) {
	if (value) {
		flag |= 0x01;
	} else {
		flag &= ~0x01;
	}
	return value;
}

inline bool geometry::set_m(const bool value) {
	if (value) {
		flag |= 0x02;
	} else {
		flag &= ~0x02;
	}
	return value;
}

inline bool geometry::is_single_part() const {
	return type == geometry_type::POINT || type == geometry_type::LINESTRING;
}

inline bool geometry::is_multi_part() const {
	return type >= geometry_type::POLYGON && type <= geometry_type::MULTI_GEOMETRY;
}

inline bool geometry::is_collection() const {
	return type >= geometry_type::MULTI_POINT && type <= geometry_type::MULTI_GEOMETRY;
}

inline uint32_t geometry::get_count() const {
	return size;
}

inline void geometry::set_count(const uint32_t count) {
	size = count;
}

inline bool geometry::is_empty() const {
	return size == 0;
}

inline const geometry *geometry::get_last_part() const {
	SGL_ASSERT(is_multi_part() || type == geometry_type::INVALID);
	const auto tail = static_cast<geometry *>(data);
	return tail;
}

inline const geometry *geometry::get_first_part() const {
	const auto tail = get_last_part();
	return tail ? tail->next : nullptr;
}

inline const geometry *geometry::get_nth_part(uint32_t n) const {
	if (size == 0) {
		SGL_ASSERT(data == nullptr);
		return nullptr;
	}

	auto part = get_first_part();
	SGL_ASSERT(part != nullptr);

	for (uint32_t i = 0; i < n; i++) {
		part = part->next;
		SGL_ASSERT(part != nullptr);
	}

	return part;
}

inline const geometry *geometry::get_next() const {
	return next;
}

inline const geometry *geometry::get_parent() const {
	return prnt;
}

inline geometry *geometry::get_last_part() {
	return const_cast<geometry *>(static_cast<const geometry *>(this)->get_last_part());
}
inline geometry *geometry::get_first_part() {
	return const_cast<geometry *>(static_cast<const geometry *>(this)->get_first_part());
}
inline geometry *geometry::get_nth_part(uint32_t n) {
	return const_cast<geometry *>(static_cast<const geometry *>(this)->get_nth_part(n));
}
inline geometry *geometry::get_next() {
	return const_cast<geometry *>(static_cast<const geometry *>(this)->get_next());
}
inline geometry *geometry::get_parent() {
	return const_cast<geometry *>(static_cast<const geometry *>(this)->get_parent());
}

inline void geometry::append_part(geometry *part) {
	SGL_ASSERT(is_multi_part() || type == geometry_type::INVALID);
	SGL_ASSERT(part != nullptr);

	const auto tail = static_cast<geometry *>(data);

	if (tail == nullptr) {
		SGL_ASSERT(size == 0);
		part->next = part;
	} else {
		SGL_ASSERT(size != 0);
		const auto head = tail->next;
		tail->next = part;
		part->next = head;
	}

	part->prnt = this;
	data = part;
	size++;
}

// This needs testing
inline void geometry::filter_parts(void *state, select_func select, handle_func handle) {
	auto tail = get_last_part();

	if (!tail) {
		return;
	}

	auto prev = tail;
	bool shrank = true;

	while (size > 0 && (prev != tail || shrank)) {
		shrank = false;
		auto curr = prev->next;
		auto next = curr->next;

		if (select(state, curr)) {

			// Unlink the current part
			prev->next = next;
			size--;
			shrank = true;

			if (curr == tail) {
				// We removed the tail, update the tail pointer
				tail = prev;
				data = tail;
			}

			// Before passing this to the handle function,
			// null the relationship pointers
			curr->prnt = nullptr;
			curr->next = nullptr;

			// Pass on to the handle callback
			handle(state, curr);

		} else {
			prev = curr;
		}
	}

	if (size == 0) {
		// We extracted everything. Reset the data pointer
		data = nullptr;
	}
}

inline geometry *geometry::pop_first_part() {
	const auto tail = get_last_part();

	if (tail == nullptr) {
		// No parts
		SGL_ASSERT(size == 0);
		return nullptr;
	}

	const auto head = tail->next;
	SGL_ASSERT(head != nullptr);

	// Unlink the head
	tail->next = head->next;
	head->prnt = nullptr;
	head->next = nullptr;

	size--;

	if (tail == head) {
		// Special case: this was the last element, reset the data pointer
		SGL_ASSERT(size == 0);
		data = nullptr;
	}

	return head;
}

inline const uint8_t *geometry::get_vertex_data() const {
	SGL_ASSERT(is_single_part() || type == geometry_type::INVALID);
	return static_cast<const uint8_t *>(data);
}

inline uint8_t *geometry::get_vertex_data() {
	SGL_ASSERT(is_single_part() || type == geometry_type::INVALID);
	return static_cast<uint8_t *>(data);
}

inline void geometry::set_vertex_data(const uint8_t *data, uint32_t size) {
	SGL_ASSERT(is_single_part() || type == geometry_type::INVALID);
	// Points can have at most one vertex
	SGL_ASSERT(type != geometry_type::POINT || size < 2);
	this->data = const_cast<uint8_t *>(data);
	this->size = size;
}

inline void geometry::set_vertex_data(const char *data, uint32_t size) {
	set_vertex_data(reinterpret_cast<const uint8_t *>(data), size);
}

inline size_t geometry::get_vertex_size() const {
	return sizeof(double) * (2 + has_z() + has_m());
}

inline vertex_xy geometry::get_vertex_xy(const uint32_t n) const {
	SGL_ASSERT(is_single_part() || type == geometry_type::INVALID);
	SGL_ASSERT(n < size);

	const auto vertex_stride = get_vertex_size();
	const auto vertex_buffer = get_vertex_data();
	const auto vertex_offset = vertex_buffer + vertex_stride * n;

	vertex_xy vertex = {0};
	memcpy(&vertex, vertex_offset, sizeof(vertex_xy));
	return vertex;
}

inline vertex_xyzm geometry::get_vertex_xyzm(const uint32_t n) const {
	SGL_ASSERT(is_single_part() || type == geometry_type::INVALID);
	SGL_ASSERT(n < size);

	const auto vertex_stride = get_vertex_size();
	const auto vertex_buffer = get_vertex_data();
	const auto vertex_offset = vertex_buffer + vertex_stride * n;

	vertex_xyzm vertex = {0, 0, 0, 0};
	memcpy(&vertex, vertex_offset, vertex_stride);
	return vertex;
}

inline void geometry::set_vertex_xy(const uint32_t n, const vertex_xy &vertex) {
	SGL_ASSERT(is_single_part() || type == geometry_type::INVALID);
	SGL_ASSERT(n < size);

	const auto vertex_stride = get_vertex_size();
	const auto vertex_buffer = get_vertex_data();
	const auto vertex_offset = vertex_buffer + vertex_stride * n;

	memcpy(vertex_offset, &vertex, sizeof(vertex_xy));
}

inline std::string geometry::type_to_string(const geometry_type type) {
	switch (type) {
	case geometry_type::POINT:
		return "POINT";
	case geometry_type::LINESTRING:
		return "LINESTRING";
	case geometry_type::POLYGON:
		return "POLYGON";
	case geometry_type::MULTI_POINT:
		return "MULTIPOINT";
	case geometry_type::MULTI_LINESTRING:
		return "MULTILINESTRING";
	case geometry_type::MULTI_POLYGON:
		return "MULTIPOLYGON";
	case geometry_type::MULTI_GEOMETRY:
		return "GEOMETRYCOLLECTION";
	default:
		return "INVALID";
	}
}

} // namespace sgl

//----------------------------------------------------------------------------------------------------------------------
// Prepared Geometry
//----------------------------------------------------------------------------------------------------------------------
namespace sgl {

struct n_index {
	static constexpr auto NODE_SIZE = 16;

	struct level {
		box_xy*  entry_array = nullptr;
		uint32_t entry_count = 0;
	};

	level*   level_array = nullptr;
	uint32_t level_count = 0;
	bool is_ccw = false;

	void build(allocator* alloc, uint32_t vertex_width, const void *vertex_array, uint32_t vertex_count);
	bool contains(const vertex_xy *v, uint32_t vertex_width, const void *vertex_array) const;
	double distance(const vertex_xy *v, uint32_t vertex_width, const void *vertex_array) const;
};


struct prepared_geometry : geometry {

	prepared_geometry() = default;
	explicit prepared_geometry(const geometry_type type, const bool has_z = false, const bool has_m = false)
		: geometry(type, has_z, has_m) {
	}

	n_index	index = {};
};

}
//--------------------------------------------------------------------------
// Operations
//--------------------------------------------------------------------------

namespace sgl {

namespace point {
inline void init_empty(sgl::geometry *geom, bool has_z = false, bool has_m = false) {
	geom->set_type(geometry_type::POINT);
	geom->set_z(has_z);
	geom->set_m(has_m);
}
} // namespace point

namespace linestring {
inline void init_empty(sgl::geometry *geom, bool has_z = false, bool has_m = false) {
	geom->set_type(geometry_type::LINESTRING);
	geom->set_z(has_z);
	geom->set_m(has_m);
}

inline bool is_closed(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::LINESTRING);

	if (geom->get_count() < 2) {
		return false;
	}

	const auto first = geom->get_vertex_xyzm(0);
	const auto last = geom->get_vertex_xyzm(geom->get_count() - 1);
	return first == last;
}

// returns positive result if oriented clockwise
inline double signed_area(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::LINESTRING);
	SGL_ASSERT(is_closed(geom));

	const auto count = geom->get_count();

	if (count < 3) {
		return 0.0;
	}

	const auto vertex_data = geom->get_vertex_data();
	const auto vertex_size = geom->get_vertex_size();

	auto area = 0.0;

	double x0 = 0.0;
	double x1 = 0.0;
	double y1 = 0.0;
	double y2 = 0.0;

	const auto x_data = vertex_data;
	const auto y_data = vertex_data + sizeof(double);

	memcpy(&x0, x_data, sizeof(double));

	for (uint32_t i = 1; i < count - 1; i++) {
		memcpy(&x1, x_data + (i + 0) * vertex_size, sizeof(double));
		memcpy(&y1, y_data + (i + 1) * vertex_size, sizeof(double));
		memcpy(&y2, y_data + (i - 1) * vertex_size, sizeof(double));

		area += (x1 - x0) * (y2 - y1);
	}

	return area * 0.5;
}

inline double length(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::LINESTRING);

	const auto count = geom->get_count();
	if (count < 2) {
		return 0.0;
	}

	const auto vertex_data = geom->get_vertex_data();
	const auto vertex_size = geom->get_vertex_size();

	auto length = 0.0;

	vertex_xy prev = {0};
	vertex_xy next = {0};

	memcpy(&prev, vertex_data, sizeof(vertex_xy));

	for (uint32_t i = 1; i < count; i++) {
		memcpy(&next, vertex_data + i * vertex_size, sizeof(vertex_xy));
		const auto dx = next.x - prev.x;
		const auto dy = next.y - prev.y;
		length += std::hypot(dx, dy);
		prev = next;
	}

	return length;
}

// geom must be a non-empty linestring
// returns false if empty
bool interpolate(const sgl::geometry *geom, double frac, vertex_xyzm *out);

// returns a multipoint with interpolated points
void interpolate_points(sgl::geometry *result, sgl::allocator *alloc, const sgl::geometry *geom, double frac);
void substring(sgl::geometry *result,sgl::allocator *alloc, const sgl::geometry *geom, double beg_frac, double end_frac);

} // namespace linestring

namespace polygon {

inline void init_empty(geometry *geom, bool has_z = false, bool has_m = false) {
	geom->set_type(geometry_type::POLYGON);
    geom->set_z(has_z);
    geom->set_m(has_m);
}

inline double area(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::POLYGON);
	double area = 0.0;

	auto part = geom->get_first_part();
	if (!part) {
		return area;
	}

	area += std::abs(linestring::signed_area(part));

	while (part != geom->get_last_part()) {
		part = part->get_next();
		area -= std::abs(linestring::signed_area(part));
	}

	return area;
}

inline double perimeter(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::POLYGON);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}

	double perimeter = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		perimeter += linestring::length(part);
	} while (part != tail);

	return perimeter;
}

inline void init_from_box(sgl::geometry *geom, sgl::allocator *alloc, double minx, double miny, double maxx, double maxy) {
	geom->set_type(geometry_type::POLYGON);
	geom->set_z(false);
	geom->set_m(false);

	const auto ring_mem = alloc->alloc(sizeof(sgl::geometry));
	const auto ring_ptr = new (ring_mem) sgl::geometry(sgl::geometry_type::LINESTRING, false, false);

	const auto data_mem = alloc->alloc(2 * sizeof(double) * 5);
	const auto data_ptr = static_cast<double*>(data_mem);

	data_ptr[0] = minx;
	data_ptr[1] = miny;

	data_ptr[2] = minx;
	data_ptr[3] = maxy;

	data_ptr[4] = maxx;
	data_ptr[5] = maxy;

	data_ptr[6] = maxx;
	data_ptr[7] = miny;

	data_ptr[8] = minx;
	data_ptr[9] = miny;

	ring_ptr->set_vertex_data(static_cast<const uint8_t*>(data_mem), 5);
	geom->append_part(ring_ptr);
}

} // namespace polygon

namespace multi_point {

inline void init_empty(sgl::geometry *geom, bool has_z = false, bool has_m = false) {
    geom->set_type(geometry_type::MULTI_POINT);
    geom->set_z(has_z);
    geom->set_m(has_m);}

} // namespace multi_point

namespace multi_linestring {
inline void init_empty(sgl::geometry *geom, bool has_z = false, bool has_m = false) {
	geom->set_type(geometry_type::MULTI_LINESTRING);
	geom->set_z(has_z);
	geom->set_m(has_m);
}

inline bool is_closed(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_LINESTRING);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return false;
	}

	auto part = tail;
	do {
		part = part->get_next();
		if (!linestring::is_closed(part)) {
			return false;
		}
	} while (part != tail);

	return true;
}

inline double length(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_LINESTRING);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}

	double length = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		length += linestring::length(part);
	} while (part != tail);

	return length;
}

} // namespace multi_linestring

namespace multi_polygon {
inline void init_empty(sgl::geometry *geom, bool has_z = false, bool has_m = false) {
	geom->set_type(geometry_type::MULTI_POLYGON);
    geom->set_z(has_z);
    geom->set_m(has_m);
}

inline double area(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_POLYGON);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}

	double area = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		area += polygon::area(part);
	} while (part != tail);

	return area;
}

inline double perimeter(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_POLYGON);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}

	double perimeter = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		perimeter += polygon::perimeter(part);
	} while (part != tail);

	return perimeter;
}

} // namespace multi_polygon

namespace multi_geometry {

inline void init_empty(sgl::geometry *geom, bool has_z = false, bool has_m = false) {
	geom->set_type(geometry_type::MULTI_GEOMETRY);
    geom->set_z(has_z);
    geom->set_m(has_m);
}
inline double area(const geometry *geom);
inline double length(const geometry *geom);
inline double perimeter(const geometry *geom);

} // namespace multi_geometry

namespace ops {

// If the allocator is null, transform in-place. Otherwise allocate new vertex data
void affine_transform(sgl::allocator *alloc, sgl::geometry *geom, const sgl::affine_matrix *matrix);

double area(const geometry *geom);
double perimeter(const geometry *geom);
double length(const geometry *geom);
size_t vertex_count(const geometry *geom);
int32_t max_surface_dimension(const geometry *geom, bool ignore_empty);

double euclidean_distance(const geometry* lhs, const geometry* rhs);

// This will NOT visit polygon rings if the requested dimension is 1
typedef void (*visit_func)(void *state, const geometry *part);
void visit_by_dimension(const geometry *geom, int surface_dimension, void *state, visit_func func);

typedef void (*map_vertex_func)(void *state, vertex_xyzm *vertex);
void replace_vertices(allocator *alloc, geometry *geom, void *state, map_vertex_func callback);

box_xy extent_xy(const geometry *geom);
void force_zm(allocator &alloc, geometry *geom, bool has_z, bool has_m, double default_z, double default_m);

size_t to_wkb_size(const geometry *geom);
size_t to_wkb(const geometry *geom, uint8_t *buffer, size_t size);

enum SGL_WKB_READER_ERROR {
	SGL_WKB_READER_OK = 0,
	SGL_WKB_READER_UNSUPPORTED_TYPE = 1,
	SGL_WKB_READER_OUT_OF_BOUNDS = 2,
	SGL_WKB_READER_RECURSION_LIMIT = 3,
	SGL_WKB_READER_MIXED_ZM = 4,
	SGL_WKB_INVALID_CHILD_TYPE = 5,
};

struct wkb_reader {
	// Set by the user
	allocator *alloc;
	const char *buf;
	const char *end;
	bool copy_vertices;
	bool allow_mixed_zm;
	bool nan_as_empty;

	uint32_t *stack_buf;
	uint32_t stack_cap;

	// Set by the parser
	const char *pos;
	size_t depth;
	SGL_WKB_READER_ERROR error;

	uint32_t type_id;
	bool le;
	bool has_mixed_zm;
	bool has_any_z;
	bool has_any_m;
};

bool wkb_reader_try_parse(wkb_reader *state, geometry *out);
std::string wkb_reader_get_error_message(const wkb_reader *state);

// Does not require an allocator
bool wkb_reader_try_parse_stats(wkb_reader *state, box_xy *out_extent, size_t *out_vertex_count);

struct wkt_reader {
	// Set by the user
	allocator *alloc;
	const char *buf;
	const char *end;

	// Set by the parser
	const char *pos;
	const char *error;
};

bool wkt_reader_try_parse(wkt_reader *state, geometry *out);
std::string wkt_reader_get_error_message(const wkt_reader *state);

void extract_points(sgl::geometry *result, sgl::geometry *geom);
void extract_linestrings(sgl::geometry *result, sgl::geometry *geom);
void extract_polygons(sgl::geometry *result, sgl::geometry *geom);

// TODO: this will only check that geometries have enough vertices to be valid.
// It does NOT check topological validity.
bool is_valid(const sgl::geometry *geom);

bool get_centroid(const sgl::geometry *geom, vertex_xyzm *out);

} // namespace ops

} // namespace sgl

//--------------------------------------------------------------------------
// Implementation
//--------------------------------------------------------------------------

namespace sgl {
namespace ops {

inline void affine_transform(sgl::allocator *alloc, sgl::geometry *geom, const sgl::affine_matrix *matrix) {
	if (!geom) {
		return;
	}

	geometry *part = geom;
	geometry *root = part->get_parent();

	while (true) {
		switch (part->get_type()) {
		case geometry_type::POINT:
		case geometry_type::LINESTRING: {
			const auto vertex_width = part->get_vertex_size();
			const auto vertex_count = part->get_count();

			if(vertex_count == 0) {
				break;
			}

			const auto old_vertex_data = part->get_vertex_data();
			auto new_vertex_data = old_vertex_data;
			if(alloc) {
				new_vertex_data = static_cast<uint8_t*>(alloc->alloc(vertex_width * vertex_count));
			}

			// Now, apply the transformation
			vertex_xyzm old_vertex = {0, 0, 1, 1};
			for (uint32_t i = 0; i < vertex_count; i++) {
				memcpy(&old_vertex, old_vertex_data + i * vertex_width, vertex_width);
                auto new_vertex = matrix->apply_xyz(old_vertex);
                memcpy(new_vertex_data + i * vertex_width, &new_vertex, vertex_width);
            }

			part->set_vertex_data(new_vertex_data, vertex_count);
		}
		break;
		case geometry_type::POLYGON:
		case geometry_type::MULTI_POINT:
		case geometry_type::MULTI_LINESTRING:
		case geometry_type::MULTI_POLYGON:
		case geometry_type::MULTI_GEOMETRY:
			if (!part->is_empty()) {
				part = part->get_first_part();
				continue;
			}
			break;
		default:
			SGL_ASSERT(false);
			return;
		}

		while (true) {
			const auto parent = part->get_parent();
			if (parent == root) {
				return;
			}

			if (part != parent->get_last_part()) {
				part = part->get_next();
				break;
			}

			part = parent;
		}
	}
}


inline double area(const geometry *geom) {
	switch (geom->get_type()) {
	case geometry_type::POLYGON: {
		return polygon::area(geom);
	}
	case geometry_type::MULTI_POLYGON: {
		return multi_polygon::area(geom);
	}
	case geometry_type::MULTI_GEOMETRY: {
		return multi_geometry::area(geom);
	}
	default:
		return 0.0;
	}
}

inline double perimeter(const geometry *geom) {
	switch (geom->get_type()) {
	case geometry_type::POLYGON: {
		return polygon::perimeter(geom);
	}
	case geometry_type::MULTI_POLYGON: {
		return multi_polygon::perimeter(geom);
	}
	case geometry_type::MULTI_GEOMETRY: {
		return multi_geometry::perimeter(geom);
	}
	default:
		return 0.0;
	}
}

inline double length(const geometry *geom) {
	switch (geom->get_type()) {
	case geometry_type::LINESTRING: {
		return linestring::length(geom);
	}
	case geometry_type::MULTI_LINESTRING: {
		return multi_linestring::length(geom);
	}
	case geometry_type::MULTI_GEOMETRY: {
		return multi_geometry::length(geom);
	}
	default:
		return 0;
	}
}

inline size_t vertex_count(const geometry *geom) {
	if (!geom) {
		return 0;
	}

	size_t count = 0;
	const geometry *part = geom;
	const geometry *root = part->get_parent();

	while (true) {
		switch (part->get_type()) {
		case geometry_type::POINT:
		case geometry_type::LINESTRING:
			count += part->get_count();
			break;
		case geometry_type::POLYGON:
		case geometry_type::MULTI_POINT:
		case geometry_type::MULTI_LINESTRING:
		case geometry_type::MULTI_POLYGON:
		case geometry_type::MULTI_GEOMETRY:
			if (!part->is_empty()) {
				part = part->get_first_part();
				continue;
			}
			break;
		default:
			SGL_ASSERT(false);
			return 0;
		}

		while (true) {
			const auto parent = part->get_parent();
			if (parent == root) {
				return count;
			}

			if (part != parent->get_last_part()) {
				part = part->get_next();
				break;
			}

			part = parent;
		}
	}
	return count;
}

inline int32_t max_surface_dimension(const geometry *geom, bool ignore_empty) {
	if (!geom) {
		return 0;
	}

	int32_t max_dim = 0;
	const geometry *part = geom;
	const geometry *root = part->get_parent();

	while (true) {
		if (!(part->is_empty() && ignore_empty)) {
			switch (part->get_type()) {
			case geometry_type::POINT:
			case geometry_type::MULTI_POINT:
				max_dim = std::max(max_dim, 0);
				break;
			case geometry_type::LINESTRING:
			case geometry_type::MULTI_LINESTRING:
				max_dim = std::max(max_dim, 1);
				break;
			case geometry_type::POLYGON:
			case geometry_type::MULTI_POLYGON:
				max_dim = std::max(max_dim, 2);
				break;
			case geometry_type::MULTI_GEOMETRY:
				if (!part->is_empty()) {
					part = part->get_first_part();
					continue;
				}
				break;
			default:
				SGL_ASSERT(false);
				return 0;
			}
		}

		while (true) {
			const auto parent = part->get_parent();
			if (parent == root) {
				return max_dim;
			}

			if (part != parent->get_last_part()) {
				part = part->get_next();
				break;
			}

			part = parent;
		}
	}
}

inline void visit_by_dimension(const geometry *geom, int surface_dimension, void *state, visit_func func) {
	if (!geom) {
		return;
	}

	const geometry *part = geom;
	const geometry *root = part->get_parent();

	while (true) {
		switch (part->get_type()) {
		case geometry_type::POINT:
			if(surface_dimension == 0) {
				func(state, part);
			}
			break;
		case geometry_type::MULTI_POINT:
			if(surface_dimension == 0) {
				func(state, part);
				if (!part->is_empty()) {
					part = part->get_first_part();
					continue;
				}
			}
			break;
		case geometry_type::LINESTRING:
			if(surface_dimension == 1) {
                func(state, part);
            }
		break;
		case geometry_type::MULTI_LINESTRING:
			if(surface_dimension == 1) {
				func(state, part);
				if (!part->is_empty()) {
					part = part->get_first_part();
					continue;
				}
			}
		break;
		case geometry_type::POLYGON:
			if(surface_dimension == 2) {
				func(state, part);
			}
			break;
		case geometry_type::MULTI_POLYGON:
			if(surface_dimension == 2) {
				func(state, part);
				if (!part->is_empty()) {
                    part = part->get_first_part();
                    continue;
                }
			}
			break;
		case geometry_type::MULTI_GEOMETRY:
			if (!part->is_empty()) {
				part = part->get_first_part();
				continue;
			}
			break;
		default:
			SGL_ASSERT(false);
			return;
		}

		while (true) {
			const auto parent = part->get_parent();
			if (parent == root) {
				return;
			}

			if (part != parent->get_last_part()) {
				part = part->get_next();
				break;
			}

			part = parent;
		}
	}
}

inline void replace_vertices(allocator *alloc, geometry *geom, void *state, map_vertex_func callback) {
	if (!geom) {
		return;
	}

	geometry *part = geom;
	const geometry *root = part->get_parent();

	while (true) {
		switch (part->get_type()) {
		case geometry_type::POINT:
		case geometry_type::LINESTRING: {
			const auto vertex_count =  part->get_count();
			if(vertex_count == 0) {
				break;
			}
			const auto vertex_size = part->get_vertex_size();
			const auto old_vertex_data = part->get_vertex_data();
			const auto new_vertex_data = static_cast<uint8_t*>(alloc->alloc(part->get_count() * vertex_size));

			vertex_xyzm vertex = {0, 0, 0, 0};
			for(uint32_t v_idx = 0; v_idx < part->get_count(); v_idx++) {
				memcpy(&vertex, old_vertex_data + v_idx * vertex_size, vertex_size);
				callback(state, &vertex);
				memcpy(new_vertex_data + v_idx * vertex_size, &vertex, vertex_size);
			}
			part->set_vertex_data(new_vertex_data, vertex_count);
		} break;
		case geometry_type::POLYGON:
		case geometry_type::MULTI_POINT:
		case geometry_type::MULTI_LINESTRING:
		case geometry_type::MULTI_POLYGON:
		case geometry_type::MULTI_GEOMETRY: {
			if (!part->is_empty()) {
				part = part->get_first_part();
				continue;
			}
		} break;
		default:
			SGL_ASSERT(false);
			return;
		}

		while (true) {
			const auto parent = part->get_parent();
			if (parent == root) {
				// Done!
				return;
			}

			if (part != parent->get_last_part()) {
				part = part->get_next();
				break;
			}

			part = parent;
		}
	}
}

template <class F>
inline void visit_vertices(const geometry *geom, F callback) {
	switch (geom->get_type()) {
	case geometry_type::POINT:
	case geometry_type::LINESTRING: {
		auto vertex_data = geom->get_vertex_data();
		if (vertex_data == nullptr) {
			return;
		}
		auto vertex_size = geom->get_vertex_size();
		for (uint32_t i = 0; i < geom->get_count(); i++) {
			callback(vertex_data + i * vertex_size);
		}
	}
		return;
	case geometry_type::POLYGON:
	case geometry_type::MULTI_POINT:
	case geometry_type::MULTI_LINESTRING:
	case geometry_type::MULTI_POLYGON:
	case geometry_type::MULTI_GEOMETRY: {
		const auto tail = geom->get_last_part();
		if (!tail) {
			return;
		}
		auto part = tail;
		do {
			part = part->get_next();
			visit_vertices<F>(part, callback);
		} while (part != tail);
	}
		return;
	default:
		return;
	}
}

// Non-recursive
inline bool try_get_extent_xy(const geometry *geom, box_xy *out) {

	auto part = geom;
	if (part == nullptr) {
		return false;
	}

	box_xy result = box_xy::smallest();

	const auto root = part->get_parent();
	bool has_any_vertices = false;

	while (true) {
		switch (part->get_type()) {
		case geometry_type::POINT:
		case geometry_type::LINESTRING: {
			const auto vertex_count = part->get_count();

			has_any_vertices |= vertex_count > 0;

			for (uint32_t i = 0; i < vertex_count; i++) {
				const auto vertex = part->get_vertex_xy(i);
				result.min.x = std::min(result.min.x, vertex.x);
				result.min.y = std::min(result.min.y, vertex.y);
				result.max.x = std::max(result.max.x, vertex.x);
				result.max.y = std::max(result.max.y, vertex.y);
			}
		} break;
		case geometry_type::POLYGON: {
			if (!part->is_empty()) {
				const auto shell = part->get_first_part();
				const auto vertex_count = shell->get_count();
				has_any_vertices |= vertex_count > 0;
				for (uint32_t i = 0; i < vertex_count; i++) {
					const auto vertex = shell->get_vertex_xy(i);
					result.min.x = std::min(result.min.x, vertex.x);
					result.min.y = std::min(result.min.y, vertex.y);
					result.max.x = std::max(result.max.x, vertex.x);
					result.max.y = std::max(result.max.y, vertex.y);
				}
			}
		} break;
		case geometry_type::MULTI_POINT:
		case geometry_type::MULTI_LINESTRING:
		case geometry_type::MULTI_POLYGON:
		case geometry_type::MULTI_GEOMETRY: {
			if (!part->is_empty()) {
				part = part->get_first_part();
				// continue the outer loop here!
				continue;
			}
		} break;
		default:
			SGL_ASSERT(false);
			return false;
		}

		// Now go up/sideways
		while (true) {
			const auto parent = part->get_parent();
			if (parent == root) {
				if (has_any_vertices) {
					*out = result;
					return true;
				}
				return false;
			}

			if (part != parent->get_last_part()) {
				// Go sideways
				part = part->get_next();
				break;
			}

			// Go up
			part = parent;
		}
	}
}

// Result for visit action
enum SGL_VISIT_RESULT {
	// Continue the traversal, recurse down into child parts
	SGL_VISIT_CONT = 0,
	// Stop the traversal immediately
	SGL_VISIT_EXIT = 1,
	// Continue the traversal, but skip the current part and dont recurse down
	SGL_VISIT_SKIP = 2,
};

struct visit_callbacks {
	// Return false to stop the traversal
	SGL_VISIT_RESULT (*on_enter_part)(void *state, const geometry *part, const geometry *parent) = nullptr;
	SGL_VISIT_RESULT (*on_leave_part)(void *state, const geometry *part, const geometry *parent) = nullptr;
};

inline void visit(const geometry *geom, const geometry *root, const visit_callbacks *visitor, void *state) {

#define HANDLE_ENTER_PART(PART, PARENT)                                                                                \
	if (visitor->on_enter_part) {                                                                                      \
		SGL_VISIT_RESULT res = visitor->on_enter_part(state, PART, PARENT);                                            \
		if (res == SGL_VISIT_EXIT) {                                                                                   \
			return;                                                                                                    \
		} else if (res == SGL_VISIT_SKIP) {                                                                            \
			break;                                                                                                     \
		}                                                                                                              \
	}
#define HANDLE_LEAVE_PART(PART, PARENT)                                                                                \
	if (visitor->on_leave_part) {                                                                                      \
		SGL_VISIT_RESULT res = visitor->on_leave_part(state, PART, PARENT);                                            \
		if (res == SGL_VISIT_EXIT) {                                                                                   \
			return;                                                                                                    \
		} else if (res == SGL_VISIT_SKIP) {                                                                            \
			break;                                                                                                     \
		}                                                                                                              \
	}
#define HANDLE_ENTER_CHILD_PART(PART, PARENT)                                                                          \
	if (visitor->on_enter_part) {                                                                                      \
		SGL_VISIT_RESULT res = visitor->on_enter_part(state, PART, PARENT);                                            \
		if (res == SGL_VISIT_EXIT) {                                                                                   \
			return;                                                                                                    \
		} else if (res == SGL_VISIT_SKIP) {                                                                            \
			continue;                                                                                                  \
		}                                                                                                              \
	}
#define HANDLE_LEAVE_CHILD_PART(PART, PARENT)                                                                          \
	if (visitor->on_leave_part) {                                                                                      \
		SGL_VISIT_RESULT res = visitor->on_leave_part(state, PART, PARENT);                                            \
		if (res == SGL_VISIT_EXIT) {                                                                                   \
			return;                                                                                                    \
		} else if (res == SGL_VISIT_SKIP) {                                                                            \
			continue;                                                                                                  \
		}                                                                                                              \
	}

	auto part = geom;
	if (part == nullptr) {
		return;
	}

	auto parent = part->get_parent();

	while (true) {
		switch (part->get_type()) {
		case geometry_type::POINT:
		case geometry_type::LINESTRING: {
			HANDLE_ENTER_PART(part, parent);
			HANDLE_LEAVE_PART(part, parent);
		} break;
		case geometry_type::POLYGON: {

			HANDLE_ENTER_PART(part, parent);

			const auto tail = part->get_last_part();
			if (tail != nullptr) {
				auto head = tail;
				do {
					SGL_ASSERT(head != nullptr);
					SGL_ASSERT(head->get_type() == geometry_type::LINESTRING);

					head = head->get_next();

					HANDLE_ENTER_CHILD_PART(head, part);
					HANDLE_LEAVE_CHILD_PART(head, part);

				} while (head != tail);
			}

			HANDLE_LEAVE_PART(part, parent);
		} break;
		case geometry_type::MULTI_POINT: {
			HANDLE_ENTER_PART(part, parent);

			const auto tail = part->get_last_part();
			if (tail != nullptr) {
				auto head = tail;
				do {
					SGL_ASSERT(head != nullptr);
					SGL_ASSERT(head->get_type() == geometry_type::POINT);

					head = head->get_next();

					HANDLE_ENTER_CHILD_PART(head, part);
					HANDLE_LEAVE_CHILD_PART(head, part);

				} while (head != tail);
			}
			HANDLE_LEAVE_PART(part, parent);

		} break;
		case geometry_type::MULTI_LINESTRING: {
			HANDLE_ENTER_PART(part, parent);

			const auto tail = part->get_last_part();
			if (tail != nullptr) {
				auto head = tail;
				do {
					SGL_ASSERT(head != nullptr);
					SGL_ASSERT(head->get_type() == geometry_type::LINESTRING);

					head = head->get_next();

					HANDLE_ENTER_CHILD_PART(head, part);
					HANDLE_LEAVE_CHILD_PART(head, part);

				} while (head != tail);
			}

			HANDLE_LEAVE_PART(part, parent);
		} break;
		case geometry_type::MULTI_POLYGON: {
			HANDLE_ENTER_PART(part, parent);

			const auto tail = part->get_last_part();
			if (tail != nullptr) {
				auto head = tail;
				do {
					SGL_ASSERT(head != nullptr);
					SGL_ASSERT(head->get_type() == geometry_type::POLYGON);

					head = head->get_next();

					HANDLE_ENTER_CHILD_PART(head, part);

					const auto ring_tail = head->get_last_part();
					if (ring_tail != nullptr) {
						auto ring_head = ring_tail;
						do {
							SGL_ASSERT(ring_head != nullptr);
							SGL_ASSERT(ring_head->get_type() == geometry_type::LINESTRING);

							ring_head = ring_head->get_next();

							HANDLE_ENTER_CHILD_PART(ring_head, head);
							HANDLE_LEAVE_CHILD_PART(ring_head, head);

						} while (ring_head != ring_tail);
					}

					HANDLE_LEAVE_CHILD_PART(head, part);

				} while (head != part->get_last_part());
			}

			HANDLE_LEAVE_PART(part, parent);
		} break;
		case geometry_type::MULTI_GEOMETRY: {
			HANDLE_ENTER_PART(part, parent);
			if (!part->is_empty()) {
				// Recurse down
				part = part->get_first_part();
				continue;
			}
			// Otherwise, end the multi geometry
			HANDLE_LEAVE_PART(part, parent);
		} break;
		default: {
			// Unknown type!
			SGL_ASSERT(false);
			return;
		}
		}

		// Now go up/sideways
		while (true) {

			if (parent == root) {
				return;
			}

			if (part != parent->get_last_part()) {
				// We should only get here if we are in a multi geometry
				SGL_ASSERT(parent->get_type() == geometry_type::MULTI_GEOMETRY);

				// Go sideways
				part = part->get_next();
				break;
			}

			// Go up
			part = parent;
			parent = part->get_parent();

			SGL_ASSERT(part->get_type() == geometry_type::MULTI_GEOMETRY);

			// We just visited the last child of a multi geometry. We should end it.
			// we use LEAVE_CHILD_PART here to trigger a continue in case we skip.
			HANDLE_LEAVE_CHILD_PART(part, parent);
		}
	}

#undef HANDLE_ENTER_PART
#undef HANDLE_LEAVE_PART
#undef HANDLE_ENTER_CHILD_PART
#undef HANDLE_LEAVE_CHILD_PART
}

} // namespace ops

} // namespace sgl

namespace sgl {
namespace multi_geometry {

inline double area(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_GEOMETRY);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}

	double area = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		area += ops::area(part);
	} while (part != tail);

	return area;
}

inline double length(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_GEOMETRY);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}
	double length = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		length += ops::length(part);
	} while (part != tail);
	return length;
}

inline double perimeter(const geometry *geom) {
	SGL_ASSERT(geom->get_type() == geometry_type::MULTI_GEOMETRY);

	const auto tail = geom->get_last_part();
	if (!tail) {
		return 0.0;
	}
	double perimeter = 0.0;
	auto part = tail;
	do {
		part = part->get_next();
		perimeter += ops::perimeter(part);
	} while (part != tail);
	return perimeter;
}

} // namespace multi_geometry

namespace util {

inline double haversine_distance(const double lat1_p, const double lon1_p, const double lat2_p, const double lon2_p) {
	// Radius of the earth in km
	constexpr auto R = 6371000.0;
	constexpr auto PI = 3.14159265358979323846;

	// Convert to radians
	const auto lat1 = lat1_p * PI / 180.0;
	const auto lon1 = lon1_p * PI / 180.0;
	const auto lat2 = lat2_p * PI / 180.0;
	const auto lon2 = lon2_p * PI / 180.0;

	const auto dlat = lat2 - lat1;
	const auto dlon = lon2 - lon1;

	const auto a =
	    std::pow(std::sin(dlat / 2.0), 2.0) + std::cos(lat1) * std::cos(lat2) * std::pow(std::sin(dlon / 2.0), 2.0);
	const auto c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));

	return R * c;
}

//----------------------------------------------------------------------------------------------------------------------
// Hilbert Curve Encoding
// From (Public Domain): https://github.com/rawrunprotected/hilbert_curves
//----------------------------------------------------------------------------------------------------------------------
inline uint32_t hilbert_interleave(uint32_t x) {
	x = (x | (x << 8)) & 0x00FF00FF;
	x = (x | (x << 4)) & 0x0F0F0F0F;
	x = (x | (x << 2)) & 0x33333333;
	x = (x | (x << 1)) & 0x55555555;
	return x;
}

inline uint32_t hilbert_encode(uint32_t n, uint32_t x, uint32_t y) {
	x = x << (16 - n);
	y = y << (16 - n);

	// Initial prefix scan round, prime with x and y
	uint32_t a = x ^ y;
	uint32_t b = 0xFFFF ^ a;
	uint32_t c = 0xFFFF ^ (x | y);
	uint32_t d = x & (y ^ 0xFFFF);
	uint32_t A = a | (b >> 1);
	uint32_t B = (a >> 1) ^ a;
	uint32_t C = ((c >> 1) ^ (b & (d >> 1))) ^ c;
	uint32_t D = ((a & (c >> 1)) ^ (d >> 1)) ^ d;

	a = A;
	b = B;
	c = C;
	d = D;
	A = ((a & (a >> 2)) ^ (b & (b >> 2)));
	B = ((a & (b >> 2)) ^ (b & ((a ^ b) >> 2)));
	C ^= ((a & (c >> 2)) ^ (b & (d >> 2)));
	D ^= ((b & (c >> 2)) ^ ((a ^ b) & (d >> 2)));

	a = A;
	b = B;
	c = C;
	d = D;
	A = ((a & (a >> 4)) ^ (b & (b >> 4)));
	B = ((a & (b >> 4)) ^ (b & ((a ^ b) >> 4)));
	C ^= ((a & (c >> 4)) ^ (b & (d >> 4)));
	D ^= ((b & (c >> 4)) ^ ((a ^ b) & (d >> 4)));

	// Final round and projection
	a = A;
	b = B;
	c = C;
	d = D;
	C ^= ((a & (c >> 8)) ^ (b & (d >> 8)));
	D ^= ((b & (c >> 8)) ^ ((a ^ b) & (d >> 8)));

	// Undo transformation prefix scan
	a = C ^ (C >> 1);
	b = D ^ (D >> 1);

	// Recover index bits
	uint32_t i0 = x ^ y;
	uint32_t i1 = b | (0xFFFF ^ (i0 | a));

	return ((hilbert_interleave(i1) << 1) | hilbert_interleave(i0)) >> (32 - 2 * n);
}

inline uint32_t hilbert_f32_to_u32(float f) {
	if (std::isnan(f)) {
		return 0xFFFFFFFF;
	}
	uint32_t res;
	memcpy(&res, &f, sizeof(res));
	if ((res & 0x80000000) != 0) {
		res ^= 0xFFFFFFFF;
	} else {
		res |= 0x80000000;
	}
	return res;
}

} // namespace util

} // namespace sgl
