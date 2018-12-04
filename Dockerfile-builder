FROM openjdk:11-jdk-slim
WORKDIR /kafka-adminrest
COPY . .
RUN ./gradlew clean build shadowJar

FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml"
COPY --from=0 /kafka-adminrest/build/libs/*.jar app.jar
