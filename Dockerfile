FROM gradle:8.10-jdk17 AS builder
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY src ./src
RUN gradle clean build -x test --no-daemon

FROM tomcat:10.1-jdk17
RUN rm -rf /usr/local/tomcat/webapps/*
COPY --from=builder /app/build/libs/SPRING_RC.war /usr/local/tomcat/webapps/ROOT.war
EXPOSE 8080
CMD ["catalina.sh", "run"]