FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml"
COPY build/install/kafka-adminrest/bin/kafka-adminrest bin/app
COPY build/install/kafka-adminrest/lib lib/
