FROM mozilla/sbt:latest

# Spark install
ENV HADOOP 2.7
ENV SPARK 2.4.7

ENV SPARK_ARCHIVE spark-${SPARK}-bin-hadoop${HADOOP}.tgz
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK}/spark-${SPARK}-bin-hadoop${HADOOP}.tgz
RUN tar -zxf ${SPARK_ARCHIVE}
RUN rm ${SPARK_ARCHIVE}

RUN echo "SPARK_HOME=/spark-${SPARK}-bin-hadoop${HADOOP}/" >> ~/.bashrc
RUN echo "PATH=$PATH:/spark-${SPARK}-bin-hadoop${HADOOP}/bin" >> ~/.bashrc

WORKDIR /app

# Data
COPY graphsParquet graphsParquet/

# Libs
COPY lib lib/
COPY lib/ target/scala-2.11/

# Configs
COPY project project/
COPY build.sbt .

# Code
COPY src src/

# Build
RUN sbt sbtVersion
RUN sbt package

# Run script
COPY start.sh .

CMD /bin/bash