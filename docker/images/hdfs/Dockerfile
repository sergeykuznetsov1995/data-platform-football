FROM apache/hadoop:3.4.1

USER root

# Install additional tools (CentOS)
RUN yum install -y curl nc && \
    yum clean all

# Create directories for HDFS
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && \
    chmod -R 755 /hadoop

# Copy scripts
COPY init-medallion.sh /usr/local/bin/init-medallion.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/init-medallion.sh /usr/local/bin/entrypoint.sh

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

WORKDIR /opt/hadoop

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
