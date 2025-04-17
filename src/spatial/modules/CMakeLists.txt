add_subdirectory(main)
add_subdirectory(proj)
add_subdirectory(gdal)
add_subdirectory(geos)
add_subdirectory(osm)
add_subdirectory(fgb)
add_subdirectory(shapefile)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
PARENT_SCOPE)