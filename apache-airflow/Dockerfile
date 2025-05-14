FROM apache/airflow:2.9.3

USER root

# Install OpenJDK for PySpark
RUN apt-get update && apt-get install -y openjdk-17-jdk wget && apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Create directory for JDBC drivers
RUN mkdir -p /opt/spark/jars

# Download PostgreSQL JDBC driver (v42.7.2, altere se necessário)
RUN wget -O /opt/spark/jars/postgresql-42.7.2.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# Permissões adequadas
RUN chown -R airflow: /opt/spark

USER airflow

# (opcional) Defina a variável do Airflow para instalar
ENV AIRFLOW_VERSION=2.9.3

# Instale o Airflow com suporte ao Spark (ajuste conforme necessário)
RUN pip install "apache-airflow[apache-spark]==$AIRFLOW_VERSION"
