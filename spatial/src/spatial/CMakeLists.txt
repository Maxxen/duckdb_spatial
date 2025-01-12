add_subdirectory(core)
add_subdirectory(gdal)
add_subdirectory(geos)
add_subdirectory(proj)
add_subdirectory(geographiclib)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/spatial_functions.cpp
        PARENT_SCOPE
)