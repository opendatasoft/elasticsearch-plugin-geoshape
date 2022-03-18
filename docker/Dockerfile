FROM docker.elastic.co/elasticsearch/elasticsearch:8.0.1 AS elasticsearch-plugin-debug

COPY /build/distributions/elasticsearch-plugin-geoshape-8.0.1.0.zip /tmp/elasticsearch-plugin-geoshape-8.0.1.0.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/elasticsearch-plugin-geoshape-8.0.1.0.zip
