FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml"
COPY build/libs/*.jar app.jar
