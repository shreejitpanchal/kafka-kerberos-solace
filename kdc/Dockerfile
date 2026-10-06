# Minimal MIT Kerberos KDC (~25 MB)
FROM alpine:3.21

RUN apk add --no-cache krb5-server krb5 bash

COPY init-kdc.sh healthcheck.sh /
# Normalise line endings in case the files were checked out with CRLF on Windows
RUN sed -i 's/\r$//' /init-kdc.sh /healthcheck.sh && chmod +x /init-kdc.sh /healthcheck.sh

ENV KRB5_CONFIG=/etc/krb5.conf \
    KRB5_KDC_PROFILE=/etc/krb5kdc/kdc.conf

EXPOSE 88/tcp 88/udp 749/tcp

ENTRYPOINT ["/init-kdc.sh"]
