FROM navikt/java:8
ENV APPLICATION_COMMIT_ID=${git_commit_id}
# As long as the class KafkaMetrics is not used (Kafka Mbean), the following comment don't need to bee in JAVA_OPTS
# -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=50367 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml"
COPY build/libs/*.jar app.jar