#!/usr/bin/env bash
# Entry point for the demo's pipeline containers: run.sh snapshot|profile|stream
set -euo pipefail

SPARK=/opt/spark/bin
CONF=/opt/anonymizer/config/demo.conf
JARS=/opt/anonymizer/jars
JOPTS="-Dconfig.file=$CONF -Dlogback.configurationFile=/opt/spark/conf/logback.xml"
# Spark on Java 17 (same opens the tests use)
OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
COMMON=(--master "local[${SPARK_CORES:-2}]" --driver-memory "${DRIVER_MEMORY:-1g}"
        --conf "spark.driver.extraJavaOptions=$JOPTS $OPENS"
        --conf spark.ui.enabled=false
        --conf spark.sql.shuffle.partitions=4)

log() { printf '\n==> %s\n' "$*"; }

case "${1:-}" in
  snapshot)
    export SNAPSHOT_PATH=file:///data/snapshot CUSTOMERS="${CUSTOMERS:-20000}"
    if [[ -f /data/snapshot/_SUCCESS ]]; then
      log "Snapshot already exists in /data/snapshot; skipping (remove the volume to regenerate)"
      exit 0
    fi
    log "Generating a synthetic customer snapshot ($CUSTOMERS customers)"
    cd /tmp  # spark-sql writes derby/metastore files to the working directory
    "$SPARK/spark-sql" "${COMMON[@]}" --conf spark.sql.catalogImplementation=in-memory \
      -f /opt/anonymizer/demo/generate-snapshot.sql
    touch /data/snapshot/_SUCCESS  # INSERT OVERWRITE DIRECTORY does not write one; the viewer waits for it
    log "Snapshot written to /data/snapshot"
    ;;
  profile)
    log "ProfileAnonymizationJob: tokenize + ARX k-anonymity / l-diversity + risk gate"
    set +e
    "$SPARK/spark-submit" "${COMMON[@]}" \
      --class com.example.anonymizer.profile.ProfileAnonymizationJob "$JARS/anonymizer-profile-job.jar"
    code=$?
    set -e
    case $code in
      0) log "Profile version published: $(cat /data/profile/_LATEST)" ;;
      2) log "Risk gate REJECTED the profile; nothing published (see /data/audit)" ;;
      *) log "Profile job failed with exit code $code" ;;
    esac
    exit $code
    ;;
  stream)
    if [[ ! -s /data/profile/_LATEST ]]; then
      log "Waiting for the profile job to publish a profile version"
      until [[ -s /data/profile/_LATEST ]]; do sleep 2; done
    fi
    log "StreamJob: Kafka -> tokenize -> join latest profile -> partitioned Parquet (trigger: ${DEMO_TRIGGER:-15 seconds})"
    exec "$SPARK/spark-submit" "${COMMON[@]}" \
      --class com.example.anonymizer.stream.StreamJob "$JARS/anonymizer-stream-job.jar"
    ;;
  *)
    echo "usage: $0 snapshot|profile|stream" >&2
    exit 1
    ;;
esac
