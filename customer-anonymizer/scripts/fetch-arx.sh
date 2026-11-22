#!/usr/bin/env bash
# Downloads the official ARX release jar, verifies it, and installs a trimmed copy into the local
# Maven repository as org.deidentifier.arx:libarx:<version>.
#
# Why trimmed: libarx-<v>.jar is a fat jar that also bundles Hadoop, Guava, BouncyCastle, commons-*
# and others, which would shadow Spark's own copies once shaded into the job jar. libarx-<v>-min.jar
# is too small: it omits de.linearbits.jhpl, which is not on Maven Central. We keep exactly ARX's own
# packages (org.deidentifier, de.linearbits); the rest come from Maven Central or the Spark runtime
# (see anonymizer-profile-job/pom.xml).
set -euo pipefail
cd "$(dirname "$0")/.."

ARX_VERSION="${ARX_VERSION:-3.9.1}"
ARX_SHA256="${ARX_SHA256:-1f16c67029970d35c9ca6f5e84da85d9efc747d2372567ffb58b54aa5e236456}"
FULL="lib/libarx-${ARX_VERSION}.jar"
CORE="lib/libarx-${ARX_VERSION}-core.jar"

mkdir -p lib
if [[ ! -f "$FULL" ]]; then
  echo "Downloading ARX ${ARX_VERSION}..."
  curl -fsSL -o "$FULL.part" \
    "https://github.com/arx-deidentifier/arx/releases/download/v${ARX_VERSION}/libarx-${ARX_VERSION}.jar"
  mv "$FULL.part" "$FULL"
fi
actual="$( (sha256sum "$FULL" 2>/dev/null || shasum -a 256 "$FULL") | awk '{print $1}')"
if [[ "$actual" != "$ARX_SHA256" ]]; then
  echo "Checksum mismatch for $FULL: expected $ARX_SHA256, got $actual" >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
(cd "$tmp" && jar xf "$OLDPWD/$FULL" org/deidentifier de/linearbits)
jar cf "$CORE" -C "$tmp" .

mvn -B -q install:install-file -Dfile="$CORE" \
  -DgroupId=org.deidentifier.arx -DartifactId=libarx -Dversion="$ARX_VERSION" -Dpackaging=jar
echo "Installed org.deidentifier.arx:libarx:${ARX_VERSION} from $CORE"
