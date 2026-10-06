# Kerberos-aware Kafka CLI toolbox, built on the same image as the broker
ARG KAFKA_IMAGE=apache/kafka:4.3.1
FROM ${KAFKA_IMAGE}

USER root
COPY bin/ /opt/tools/
# Normalise CRLF (Windows checkouts) and make the tools executable
RUN sed -i 's/\r$//' /opt/tools/* && chmod +x /opt/tools/*

ENV PATH="/opt/tools:/opt/kafka/bin:${PATH}" \
    KAFKA_OPTS="-Djava.security.krb5.conf=/etc/krb5/krb5.conf"

ENTRYPOINT ["/opt/tools/entrypoint"]
CMD ["idle"]
