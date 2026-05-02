FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app

ENV PYTHONPATH=/app
ENV SPARK_HOME=/usr/local/lib/python3.11/dist-packages/pyspark
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Delta JARs must exist before Spark starts; scoring uses --network=none (no Maven/Ivy).
RUN mkdir -p /opt/delta-jars \
    && curl -fsSL -o /opt/delta-jars/delta-spark_2.12-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    && curl -fsSL -o /opt/delta-jars/delta-storage-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
    && curl -fsSL -o /opt/delta-jars/antlr4-runtime-4.9.3.jar \
        https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
