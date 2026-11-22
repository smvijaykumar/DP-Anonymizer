#!/usr/bin/env bash
# Local end-to-end run. Needs SPARK_HOME (Spark 3.5.x), Docker, and a built project (mvn -DskipITs package).
set -euo pipefail
cd "$(dirname "$0")/.."

: "${SPARK_HOME:?Set SPARK_HOME to a Spark 3.5.x installation}"
export ANON_HMAC_KEY_V1="${ANON_HMAC_KEY_V1:-$(head -c 32 /dev/urandom | base64)}"
echo "Using a throwaway local HMAC key (tokens will differ from any other run unless you export ANON_HMAC_KEY_V1)."

JOPTS="-Dconfig.file=config/local.conf -Dlogback.configurationFile=config/logback.xml"

case "${1:-}" in
  profile)
    "$SPARK_HOME/bin/spark-submit" --master 'local[*]' --driver-memory 4g \
      --conf "spark.driver.extraJavaOptions=$JOPTS" \
      --class com.example.anonymizer.profile.ProfileAnonymizationJob \
      anonymizer-profile-job/target/anonymizer-profile-job.jar ;;
  stream)
    "$SPARK_HOME/bin/spark-submit" --master 'local[*]' \
      --conf "spark.driver.extraJavaOptions=$JOPTS" \
      --class com.example.anonymizer.stream.StreamJob \
      anonymizer-stream-job/target/anonymizer-stream-job.jar ;;
  *)
    echo "usage: $0 profile|stream" >&2; exit 1 ;;
esac
