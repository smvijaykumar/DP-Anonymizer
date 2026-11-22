# syntax=docker/dockerfile:1.6
# Build: ARX is fetched from its GitHub release and checksum-verified by scripts/fetch-arx.sh
# (a jar already in lib/ is reused).
FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /src
COPY scripts/fetch-arx.sh scripts/
COPY lib/ lib/
RUN --mount=type=cache,target=/root/.m2 ./scripts/fetch-arx.sh
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipITs package
# Runtime extras not in the Spark image: S3A connector and Logback (replaces Spark's log4j2 binding)
RUN --mount=type=cache,target=/root/.m2 for a in org.apache.hadoop:hadoop-aws:3.3.4 \
             com.amazonaws:aws-java-sdk-bundle:1.12.262 \
             ch.qos.logback:logback-classic:1.4.14 \
             ch.qos.logback:logback-core:1.4.14 \
             net.logstash.logback:logstash-logback-encoder:7.4; do \
      mvn -B -q dependency:copy -Dartifact=$a -DoutputDirectory=/extra-jars; \
    done

FROM apache/spark:3.5.3-scala2.12-java17-ubuntu
USER root
RUN rm -f /opt/spark/jars/log4j-slf4j2-impl-*.jar
COPY --from=build /extra-jars/*.jar /opt/spark/jars/
COPY config/logback.xml /opt/spark/conf/logback.xml
COPY --from=build /src/anonymizer-profile-job/target/anonymizer-profile-job.jar /opt/anonymizer/jars/
COPY --from=build /src/anonymizer-stream-job/target/anonymizer-stream-job.jar /opt/anonymizer/jars/
COPY config/ /opt/anonymizer/config/
COPY demo/pipeline/ /opt/anonymizer/demo/
# /data is the demo's shared volume; a named volume inherits this ownership on first mount
RUN mkdir -p /data && chown 185:185 /data && chmod +x /opt/anonymizer/demo/*.sh
ENV SPARK_SUBMIT_OPTS="-Dlogback.configurationFile=/opt/spark/conf/logback.xml"
USER 185
WORKDIR /opt/anonymizer
