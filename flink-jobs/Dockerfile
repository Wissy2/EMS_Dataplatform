FROM flink:1.18-scala_2.12-java11

# Install Python + PyFlink
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.18.0 && \
    # Clean up to keep the image small
    apt-get remove -y build-essential python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Updated URL for Flink 1.18 compatible Kafka connector
RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.18/flink-connector-kafka-3.0.1-1.18.jar
    
# You may also need the Kafka Clients JAR if it's not bundled
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
