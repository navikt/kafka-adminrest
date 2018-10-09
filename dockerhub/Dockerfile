FROM openjdk:11-slim
COPY . /app
WORKDIR /app
RUN ./gradlew build --no-daemon --console plain

FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml"
COPY --from=0 /app/build/install/kafka-adminrest/bin/kafka-adminrest bin/app
COPY --from=0 /app/build/install/kafka-adminrest/lib lib/
