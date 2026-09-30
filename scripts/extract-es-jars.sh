#!/usr/bin/env bash
# Pull the jars this plugin compiles against out of the stock Elasticsearch image.
# ES|QL's datasource SPI is not published to Maven, but it ships in every image.
set -euo pipefail
cd "$(dirname "$0")/.."
ES_VERSION="${ES_VERSION:-$(sed -n 's/^esVersion=//p' plugin/gradle.properties)}"
OUT=plugin/es-libs
rm -rf "$OUT" && mkdir -p "$OUT"
docker run --rm --entrypoint tar "docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION}" \
  -C /usr/share/elasticsearch -cf - lib \
  modules/x-pack-core modules/x-pack-esql modules/x-pack-esql-core modules/esql-datasource-compression-libs \
  | tar -xf - -C "$OUT"
echo "extracted ES ${ES_VERSION} jars into $OUT ($(find "$OUT" -name '*.jar' | wc -l) jars)"
