#!/usr/bin/env bash
# Build the plugin zip in a JDK 21 Gradle container (only Docker needed); it lands in dist/.
# Target another Elasticsearch version with: ES_VERSION=9.5.5 ./build-plugin.sh
set -euo pipefail
cd "$(dirname "$0")"
if [ -n "${ES_VERSION:-}" ]; then
  sed -i "s/^esVersion=.*/esVersion=${ES_VERSION}/" plugin/gradle.properties
  rm -rf plugin/es-libs
fi
[ -d plugin/es-libs/lib ] || scripts/extract-es-jars.sh
rm -rf plugin/build/distributions
docker volume create esql-splunk-gradle-cache >/dev/null
# a fresh named volume is root-owned; hand it to the calling user
docker run --rm -v esql-splunk-gradle-cache:/cache --entrypoint chown gradle:8-jdk21 "$(id -u):$(id -g)" /cache
docker run --rm -u "$(id -u):$(id -g)" -e GRADLE_USER_HOME=/cache \
  -v "$PWD":/work -v esql-splunk-gradle-cache:/cache -w /work/plugin \
  gradle:8-jdk21 gradle --no-daemon -q bundlePlugin
mkdir -p dist
cp plugin/build/distributions/*.zip dist/
ls -l dist/*.zip
