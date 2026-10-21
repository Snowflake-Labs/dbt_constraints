#!/usr/bin/env bash

# (c) Copyright [2021-2023] Open Text.
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Source: https://github.com/vertica/vertica-containers/blob/main/one-node-ce/docker-entrypoint.sh
# Changes for the dbt_constraints integration tests:
# - Use vcluster and the node management agent (NMA) instead of admintools.
#   The vertica-k8s images since 24.1 do not include admintools.
# - Generate the HTTPS and NMA certificates on first start with gen_httpstls_json.sh.
# - Set the dbadmin password from VERTICA_DBADMIN_PASSWORD. vcluster needs it.
# - Stop on the first error. A test database that is half created is useless.
# - Do not load the VMart example schema. Do not start the MC agent or crond.
# - Remove the invalid SIGKILL trap. Wait on sleep so a signal stops the loop at once.
# - Write ${READY_FILE} when startup is complete. The image HEALTHCHECK reads it.

set -e

export VERTICA_DB_USER="$(whoami)"
echo VERTICA_DB_USER is \"$VERTICA_DB_USER\"
DBADMIN_PASSWORD="${VERTICA_DBADMIN_PASSWORD:-dbadmin}"
STOP_LOOP="false"
CERT_DIR="${VERTICA_OPT_DIR}/config/https_certs"
VCLUSTER="${VERTICA_OPT_DIR}/bin/vcluster"
VCLUSTER_ARGS="--db-name ${VERTICA_DB_NAME} --hosts 127.0.0.1 --password ${DBADMIN_PASSWORD}"
VSQL="${VERTICA_OPT_DIR}/bin/vsql -U ${VERTICA_DB_USER} -w ${DBADMIN_PASSWORD}"
READY_FILE="/tmp/vertica-ready"
rm -f "${READY_FILE}"

# Vertica should be shut down properly
function shut_down() {
    echo "Shutting Down"
    vertica_proper_shutdown
    echo 'Stopping loop'
    STOP_LOOP="true"
}

function vertica_proper_shutdown() {
    if ! ${VSQL} -At -c 'SELECT 1' > /dev/null 2>&1; then
        echo "Database not running --- shutting down"
        return
    fi
    echo 'Vertica: Closing active sessions'
    ${VSQL} -c 'SELECT CLOSE_ALL_SESSIONS();'
    echo 'Vertica: Flushing everything on disk'
    ${VSQL} -c 'SELECT MAKE_AHM_NOW();'
    echo 'Vertica: Stopping database'
    ${VCLUSTER} stop_db ${VCLUSTER_ARGS}
}

function create_app_db_user() {
    echo ''
    echo "Creating APP DB user ${APP_DB_USER} ... "
    CHECK_STRING="ALREADY_EXISTS"
    CHECK_QUERY="select case when count(*) > 0 then '${CHECK_STRING}' end from users where user_name = '${APP_DB_USER}'"
    ALREADY_EXISTS=$($VSQL -c "${CHECK_QUERY}" | grep ${CHECK_STRING} | sed 's/ //g' || true)
    if [ "${ALREADY_EXISTS}" != "${CHECK_STRING}" ]; then
        ${VSQL} -c "create user ${APP_DB_USER}"
        ${VSQL} -c "grant pseudosuperuser to ${APP_DB_USER}"
        ${VSQL} -c "alter user ${APP_DB_USER} default role all"
    fi
    # Alter the user password everytime to support change of the password
    # We must prevent error "ROLLBACK 2301:  Can not reuse current password" with " || true"
    ${VSQL} -c "alter user ${APP_DB_USER} identified by '${APP_DB_PASSWORD}'" || true
}

function generate_certs() {
    # The image ships an httpstls.json that only root can read. Replace it with our own set.
    if [ -f "${CERT_DIR}/rootca.pem" ]; then
        echo "certificates already exist"
        return
    fi
    echo "Generating HTTPS and NMA certificates"
    mkdir -p "${VTMPDIR}"
    (cd "${CERT_DIR}" && rm -f httpstls.json \
        && USER="${VERTICA_DB_USER}" ${VERTICA_OPT_DIR}/bin/gen_httpstls_json.sh > /dev/null 2>&1)
    # vcluster reads rootca.pem, {username}.pem and {username}.key by default.
    cp "${CERT_DIR}/rootca_cert.pem" "${CERT_DIR}/rootca.pem"
    cp "${CERT_DIR}/${VERTICA_DB_USER}_cert.pem" "${CERT_DIR}/${VERTICA_DB_USER}.pem"
    cp "${CERT_DIR}/${VERTICA_DB_USER}_key.pem" "${CERT_DIR}/${VERTICA_DB_USER}.key"
}

function start_nma() {
    echo "Starting node management agent"
    NMA_ROOTCA_PATH="${CERT_DIR}/rootca.pem" \
    NMA_CERT_PATH="${CERT_DIR}/nma_cert.pem" \
    NMA_KEY_PATH="${CERT_DIR}/nma_key.pem" \
        nohup ${VERTICA_OPT_DIR}/bin/node_management_agent > /tmp/nma.log 2>&1 &
    for i in $(seq 1 30); do
        curl -skf https://127.0.0.1:5554/v1/health > /dev/null && return
        sleep 1
    done
    echo "ERROR: node management agent did not start"
    cat /tmp/nma.log
    exit 1
}

trap "shut_down" SIGTERM SIGHUP SIGINT
if [ -n "${TZ}" ]; then
  echo "Custom time zone required - ${TZ}"
  if [ ! -f "${VERTICA_OPT_DIR}/share/timezone/${TZ}" ]; then
    echo "ERROR: timezone file ${VERTICA_OPT_DIR}/${TZ} does not exist"
    exit 1
  fi
fi

echo 'Starting up'
generate_certs
start_nma
if [ ! -d ${VERTICA_DATA_DIR}/${VERTICA_DB_NAME} ]; then
    # first time through --- create db, etc.
    echo 'Creating database'
    ${VCLUSTER} create_db ${VCLUSTER_ARGS} \
                --catalog-path ${VERTICA_DATA_DIR} \
                --data-path ${VERTICA_DATA_DIR} \
                --config-param HttpServerConf=${CERT_DIR}/httpstls.json

    if [ -n "${APP_DB_USER}" ]; then
        create_app_db_user
    fi
else
    echo 'Starting Database'
    ${VCLUSTER} start_db ${VCLUSTER_ARGS}
fi

echo
if [ -d /docker-entrypoint-initdb.d/ ]; then
    echo "Running entrypoint scripts ..."
    for f in $(ls /docker-entrypoint-initdb.d/* | sort); do
        case "$f" in
            *.sh)     echo "$0: running $f"; . "$f" ;;
            *.sql)    echo "$0: running $f"; ${VSQL} -f $f; echo ;;
            *)        echo "$0: ignoring $f" ;;
        esac
        echo
    done
fi

echo
echo "Vertica is now running"
touch "${READY_FILE}"

while [ "${STOP_LOOP}" == "false" ]; do
    sleep 1 &
    wait $! || true
done
