FROM openjdk:8-jdk-alpine
COPY ./app.jar app.jar
CMD ["java", "-jar", "/app.jar"]