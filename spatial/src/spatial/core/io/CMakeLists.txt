add_subdirectory(osm)
add_subdirectory(shapefile)
add_subdirectory(gpkg)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        PARENT_SCOPE
)