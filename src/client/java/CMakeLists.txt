add_custom_target(
    java-client ALL
    COMMAND "mvn clean package >_build.log 2>&1"
    DEPENDS graph_thrift_obj
)

add_custom_target(
    clean-java-client
    COMMAND "mvn clean >_build.log 2>&1"
)

