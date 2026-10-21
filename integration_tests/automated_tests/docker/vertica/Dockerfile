# Single-node Vertica for the dbt_constraints integration tests.
# Base: vertica-kubernetes docker-vertica-v2 image. 25.4 is the last release that accepts the
# Community Edition license; 26.1 and later refuse CE at create_db. Multiarch: amd64 and arm64.
# Entrypoint: adapted from vertica-containers/one-node-ce.
FROM opentext/vertica-k8s:25.4.0-0-multiarch

ENV VERTICA_OPT_DIR="/opt/vertica" \
    VERTICA_DB_NAME="dbt_test" \
    VERTICA_DATA_DIR="/data/vertica" \
    VTMPDIR="/tmp/vertica"

USER root
# The operator normally supplies the run-time UID. A plain container needs a real dbadmin user.
RUN groupadd -g 5000 verticadba \
    && useradd -u 5000 -g verticadba -d /home/dbadmin -s /bin/bash dbadmin \
    && mkdir -p ${VERTICA_DATA_DIR} \
    && chown -R dbadmin:verticadba /data /home/dbadmin /opt/vertica
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

USER dbadmin
EXPOSE 5433
HEALTHCHECK --interval=3s --timeout=5s --start-period=20s --retries=60 \
    CMD test -f /tmp/vertica-ready || exit 1
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
