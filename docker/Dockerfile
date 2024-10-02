FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.24 AS elasticsearch-plugin-debug

COPY /build/distributions/elasticsearch-plugin-geoshape-7.17.24.0.zip /tmp/elasticsearch-plugin-geoshape-7.17.24.0.zip
RUN ./bin/elasticsearch-plugin install file:/tmp/elasticsearch-plugin-geoshape-7.17.24.0.zip
