FROM flink:1.18-scala_2.12-java11

# Install required system tools
RUN cp /opt/flink/opt/flink-python*.jar /opt/flink/lib/
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-dev \
      build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    # Upgrade pip to ensure --break-system-packages works
    python3 -m pip install --upgrade pip && \
    # Explicitly install into the system site-packages
    python3 -m pip install --no-cache-dir --break-system-packages \
      apache-flink==1.18.0 \
      psycopg2-binary==2.9.9 && \
    # IMMEDIATE VERIFICATION: If this fails, the build stops here.
    python3 -c "import psycopg2; print('Successfully installed psycopg2 version:', psycopg2.__version__)" && \
    # Cleanup
    apt-get remove -y build-essential python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# ── JARs — copied from local directory (no wget needed) ──────────────────────
COPY flink-connector-jdbc-3.1.2-1.18.jar      /opt/flink/lib/
COPY flink-sql-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/
COPY postgresql-42.6.0.jar                    /opt/flink/lib/

# ── Verification ─────────────────────────────────────────────────────────────
RUN python -c "import psycopg2; print(f'✓ psycopg2 {psycopg2.__version__} is ready')" && \
    echo "Checking JARs in /opt/flink/lib/:" && \
    ls -1 /opt/flink/lib/ | grep -E "kafka|jdbc|postgresql"
