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
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
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

# --- Jupyter + Lint ---

    # Install ipykernel globally for kernel registration
RUN pip install ipykernel
# Create uv project files
WORKDIR /tmp/uv-setup
RUN uv init --no-readme --name daily_dev && \
    uv add jupyter ipykernel ruff pylint pyspark pandas polars




# Register kernel as vscode
USER vscode


# Change prompt name
RUN echo 'export PS1="daily_dev ➜ %~ $ "' >> /home/vscode/.zshrc

# Register kernel
RUN python -m ipykernel install --user --name py_daily

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]