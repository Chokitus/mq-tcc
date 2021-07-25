FROM openjdk:15-alpine
COPY target/mq-tests-1.0.0-SNAPSHOT-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
