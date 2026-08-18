#!/bin/bash

SOURCE_FILE="/Users/hari-22458/Home/repos/spring_rc/build/libs/SPRING_RC.war"
DESTINATION_FILE="/Users/hari-22458/Home/apache-tomcat-11.0.13/webapps/SPRING_RC.war"

generate_war() {
    echo "Generating WAR file..."
    cd "/Users/hari-22458/Home/repos/spring_rc" || exit
    ./gradlew clean build war
}

replace_file() {
    echo "Replacing file..."
    rm -rf "$DESTINATION_FILE"
    rm -rf "/Users/hari-22458/Home/apache-tomcat-11.0.13/webapps/SPRING_RC"
    cp -f "$SOURCE_FILE" "$DESTINATION_FILE"
    echo "Waiting for Tomcat to auto-deploy..."
}

stopServer(){
  cd "/Users/hari-22458/Home/apache-tomcat-11.0.13/bin" || exit
  sh shutdown.sh
}

startServer(){
  cd "/Users/hari-22458/Home/apache-tomcat-11.0.13/bin" || exit
  export JPDA_ADDRESS="*:5005"
  export JPDA_TRANSPORT="dt_socket"
  sh catalina.sh jpda start
}

stopServer
generate_war
replace_file
startServer

echo "Done. Hit http://localhost:8080/SPRING_RC/employee"
