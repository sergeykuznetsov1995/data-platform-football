FROM apache/hadoop:3.4.1

USER root

# Install additional tools (CentOS)
RUN yum install -y curl nc && \
    yum clean all

# Create non-root hadoop user
RUN groupadd -r hadoop 2>/dev/null || true && \
    useradd -r -g hadoop -d /opt/hadoop -s /bin/bash hadoop 2>/dev/null || true

# Create directories for HDFS and set ownership
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && \
    chown -R hadoop:hadoop /hadoop && \
    chmod -R 755 /hadoop

# Copy scripts
COPY init-medallion.sh /usr/local/bin/init-medallion.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/init-medallion.sh /usr/local/bin/entrypoint.sh

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HDFS_NAMENODE_USER=hadoop
ENV HDFS_DATANODE_USER=hadoop
ENV HDFS_SECONDARYNAMENODE_USER=hadoop

# Ensure hadoop owns necessary dirs
RUN chown -R hadoop:hadoop /opt/hadoop /tmp

WORKDIR /opt/hadoop

USER hadoop

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
