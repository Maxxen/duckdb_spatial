#include "geometry/geometry.hpp"

#include <cmath>
#include <string>

namespace geometry {

template<class T>
static T Load(const uint8_t *ptr) {
	T value;
	memcpy(&value, ptr, sizeof(T));
	return value;
}

namespace operations {

double LinestringGetSignedArea(const Geometry &geom) {
	D_ASSERT(geom.GetType() == GeometryType::LINESTRING);

	const auto v_stride = geom.GetVertexSize();
	const auto v_count = geom.GetSize();
	const auto v_data = geom.GetVertices();

	if(geom.GetSize() < 3) {
		return 0.0;
	}

	auto s_area = 0.0;

	const auto x0 = Load<double>(v_data);
	for(uint32_t i = 1; i < v_count; i++) {
		const auto x1 = Load<double>(v_data + i * v_stride);
		const auto y1 = Load<double>(v_data + (i + 1) * v_stride);
		const auto y2 = Load<double>(v_data + (i - 1) * v_stride);
		s_area += (x1 - x0) * (y2 - y1);
	}
	s_area *= 0.5;
	return s_area;
}

double PolygonGetArea(const Geometry &geom) {
	D_ASSERT(geom.GetType() == GeometryType::POLYGON);

	auto first = true;
	auto sum = 0.0;
	for(const auto part : geom) {
		D_ASSERT(part->GetType() == GeometryType::LINESTRING);

		const auto r_area = fabs(LinestringGetSignedArea(*part));

		if(first) {
			sum = r_area;
			first = false;
		} else {
			sum -= r_area;
		}
	}
	return sum;
}

double MultiPolygonGetArea(const Geometry &geom) {
	D_ASSERT(geom.GetType() == GeometryType::MULTI_POLYGON);

	auto sum = 0.0;
	for(const auto part : geom) {
		D_ASSERT(part->GetType() == GeometryType::POLYGON);
		sum += PolygonGetArea(*part);
	}
	return sum;
}

double GeometryGetArea(const Geometry &geom) {
	switch(geom.GetType()) {
		case GeometryType::POLYGON:
			return PolygonGetArea(geom);
		case GeometryType::MULTI_POLYGON:
			return MultiPolygonGetArea(geom);
		case GeometryType::MULTI_GEOMETRY: {
			auto sum = 0.0;
			for(const auto part : geom) {
				sum += GeometryGetArea(*part);
			}
			return sum;
		}
		default:
			return 0.0;
	}
}

uint32_t GeometryGetNumVertices(const Geometry &geom) {
	switch(geom.GetType()) {
		case GeometryType::POINT:
		case GeometryType::LINESTRING:
			return geom.GetSize();
		case GeometryType::POLYGON:
		case GeometryType::MULTI_POINT:
		case GeometryType::MULTI_LINESTRING:
		case GeometryType::MULTI_POLYGON:
		case GeometryType::MULTI_GEOMETRY: {
			auto sum = 0;
			for(const auto part : geom) {
				sum += GeometryGetNumVertices(*part);
			}
			return sum;
		}
		default:
			D_ASSERT(false);
			return 0;
	}
}

int32_t GeometryGetDimension(const Geometry &geom) {
	if(geom.IsEmpty()) {
		return 0;
	}
	switch(geom.GetType()) {
		case GeometryType::POINT:
		case GeometryType::MULTI_POINT:
			return 0;
		case GeometryType::LINESTRING:
		case GeometryType::MULTI_LINESTRING:
			return 1;
		case GeometryType::POLYGON:
		case GeometryType::MULTI_POLYGON:
			return 2;
		case GeometryType::MULTI_GEOMETRY: {
			int32_t max = 0;
			for(const auto part : geom) {
				const auto dim = GeometryGetDimension(*part);
				if(dim > max) {
					max = dim;
				}
			}
			return max;
		}
		default:
			D_ASSERT(false);
			return 0;
	}
}

Geometry GetPointFromLine(const Geometry &geom, const uint32_t index) {
	D_ASSERT(geom.GetType() == GeometryType::LINESTRING);

	const auto v_stride = geom.GetVertexSize();
	const auto v_count = geom.GetSize();
	const auto v_data = geom.GetVertices();
	const auto count = index >= v_count ? 0 : 1;

	Geometry point(GeometryType::POINT, geom.HasZ(), geom.HasM());
	point.SetVertices(v_data + index * v_stride, count);
	return point;
}


}


} // namespace geometry