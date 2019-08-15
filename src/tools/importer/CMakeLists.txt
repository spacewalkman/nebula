add_custom_target(
    importer ALL
    COMMAND "mvn clean package > _build.log 2>&1"
    DEPENDS java-client
)
