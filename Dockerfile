FROM openjdk:15-slim
COPY target/mq-tests-1.0.0-SNAPSHOT.jar app.jar
COPY src/main/resources/benchmark benchmark
ENTRYPOINT ["java", "-jar", "app.jar"]
