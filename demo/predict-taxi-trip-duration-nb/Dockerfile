FROM centos:7 AS base

COPY zookeeper-3.4.14.tar.gz /work/
COPY openmldb-0.2.0-linux.tar.gz /work/
COPY demo /work/taxi-trip/
COPY develop_ml_application_tour.ipynb /work/

RUN yum install -y python3 python-pip libgomp java-1.8.0-openjdk-devel && yum clean all
RUN cd /work && tar -zxvf zookeeper-3.4.14.tar.gz && rm zookeeper-3.4.14.tar.gz  && cd zookeeper-3.4.14 && mv conf/zoo_sample.cfg conf/zoo.cfg
RUN cd /work && mkdir -p openmldb && tar -zxvf openmldb-0.2.0-linux.tar.gz -C openmldb --strip-components 1
RUN cd /work/openmldb/batch && tar -zxvf spark-3.0.0-bin-openmldb.tar.gz
RUN cd /work/openmldb/batch/spark-3.0.0-bin-openmldb/python && python3 setup.py install
RUN pip3 install wheel setuptools Cython numpy && pip3 install lightgbm tornado requests pandas openmldb
USER root
WORKDIR /work/taxi-trip
