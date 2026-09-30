# Elasticsearch with the Splunk bucket connector baked in.
#   docker build -f docker/Dockerfile -t elasticsearch-esql-splunk:9.5.4 .
ARG STACK_VERSION=9.5.4
FROM docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}
ARG STACK_VERSION
COPY --chown=elasticsearch:root dist/esql-datasource-splunk-*-es${STACK_VERSION}.zip /tmp/esql-datasource-splunk.zip
RUN bin/elasticsearch-plugin install --batch file:///tmp/esql-datasource-splunk.zip \
    && rm /tmp/esql-datasource-splunk.zip
