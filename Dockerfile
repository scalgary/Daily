FROM mcr.microsoft.com/devcontainers/python:3.11

# System dependencies
RUN apt-get update -qq && \
    apt-get install -y wget curl jq default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Spark env vars
ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

# Install Apache Spark
RUN cd /tmp && \
    wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    chown -R vscode:vscode ${SPARK_HOME}

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN mkdir -p ${SPARK_HOME}/logs && \
    chown -R vscode:vscode ${SPARK_HOME}/logs

RUN echo "SPARK_LOCAL_IP=127.0.0.1" >> ${SPARK_HOME}/conf/spark-env.sh && \
    echo "spark.driver.host=localhost" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.warehouse.dir=/tmp/spark-warehouse" >> ${SPARK_HOME}/conf/spark-defaults.conf

# Copy uv project files
COPY pyproject.toml uv.lock /opt/uv-project/
WORKDIR /opt/uv-project

# Install with custom venv name
ENV UV_PROJECT_ENVIRONMENT=/opt/uv-project/py_daily
RUN uv sync --frozen

# Add venv to PATH
ENV PATH="/opt/uv-project/py_daily/bin:${PATH}"

# Register kernel
RUN /opt/uv-project/py_daily/bin/python -m ipykernel install --name py_daily --display-name "py_daily"

USER vscode

# Add prompt
RUN echo 'export PS1="(py_daily) daily_dev ➜ %~ $ "' >> /home/vscode/.zshrc

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]