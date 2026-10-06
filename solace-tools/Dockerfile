# Tiny toolbox for driving the Solace broker over SEMP v2 / REST / MQTT
FROM alpine:3.21

RUN apk add --no-cache bash curl jq coreutils mosquitto-clients krb5

COPY bin/ /opt/tools/
RUN sed -i 's/\r$//' /opt/tools/* && chmod +x /opt/tools/*

ENV PATH="/opt/tools:${PATH}"
ENTRYPOINT ["/opt/tools/entrypoint"]
CMD ["status"]
