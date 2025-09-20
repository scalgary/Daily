FROM mcr.microsoft.com/devcontainers/python:3.11

# Installer les dépendances système
RUN apt-get update -qq && \
    apt-get install -y wget gnupg unzip jq curl && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | \
    gpg --dearmor -o /usr/share/keyrings/googlechrome-linux-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/googlechrome-linux-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update -qq && \
    apt-get install -y google-chrome-stable && \
    apt-get install default-jdk -y && \
    rm -rf /var/lib/apt/lists/*

# Installer uv (et le placer dans /usr/local/bin)
RUN pip install uv

# Variables d'environnement pour Spark
ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

# Installer Apache Spark
RUN cd /tmp && \
    wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    chown -R vscode:vscode ${SPARK_HOME}

# Configurer JAVA_HOME (utilise le JDK installé précédemment)
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Créer le répertoire de logs Spark
RUN mkdir -p ${SPARK_HOME}/logs && \
    chown -R vscode:vscode ${SPARK_HOME}/logs

# Optionnel : Configurer Spark pour éviter les warnings
RUN echo "SPARK_LOCAL_IP=127.0.0.1" >> ${SPARK_HOME}/conf/spark-env.sh && \
    echo "spark.driver.host=localhost" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.warehouse.dir=/tmp/spark-warehouse" >> ${SPARK_HOME}/conf/spark-defaults.conf

# S'assurer que l'utilisateur vscode peut utiliser Spark
USER vscode




