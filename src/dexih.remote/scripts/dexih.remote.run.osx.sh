#!/usr/bin/env bash
# This script downloads the remote agent binaries and runs the remote agent.

SERVER='http://localhost:5000'
DIRECTORY='remote.agent'
OS='osx';
PRE='pre'

if [ ! -d "${DIRECTORY}" ]; then
    mkdir ${DIRECTORY}
fi

# 20 is the return value of the dexih.remote if an upgrade is suggested.
UPGRADE=20

while [ ${UPGRADE} -eq 20 ]
do
    VERSION_INFO=`curl -s "${SERVER}/api/Remote/LatestVersion/${OS}/${PRE}"`
    LATEST_VERSION=$(echo "${VERSION_INFO}" | head -1 | tail -1)
    LATEST_BINARY=$(echo "${VERSION_INFO}" | head -2 | tail -1)
    LATEST_URL=$(echo "${VERSION_INFO}" | head -3 | tail -1)

    if [ -f "local_version.txt" ]; then
        LOCAL_VERSION=`more local_version.txt`
    fi

    echo LOCAL_VERSION ${LOCAL_VERSION}
    echo LATEST_VERSION ${LATEST_VERSION}
    echo LATEST_BINARY ${LATEST_BINARY}
    echo LATEST_URL ${LATEST_URL}

    if [ ! -z "${PREVIOUS_VERSION}" ] && [ "${PREVIOUS_VERSION}" -eq "${LATEST_VERSION}" ]; then
        echo The latest binary has already been downloaded, and another upgrade has been requested.
        echo The script will exit to avoid a infinte download loop.
        exit
    fi
    
    if [ "${LATEST_VERSION}" != "${LOCAL_VERSION}" ]  || [ ! -d ${DIRECTORY}  ]; then
        echo Downloading latest version from "${LATEST_URL}"
        curl -L -o "${LATEST_BINARY}" "${LATEST_URL}"

        if [ ! -f ${LATEST_BINARY} ]; then
            echo The file ${LATEST_BINARY} was not downloaded.
            exit
        fi

        # back up the appsettings
        if [ -f "${DIRECTORY}/appsettings.json" ]
        then
            mv ${DIRECTORY}/appsettings.json .
        fi

        # Clean out the previous directory contents.
        if [ -d ${DIRECTORY} ]; then
            rm -rf ${DIRECTORY}/*
        else
            mkdir ${DIRECTORY}
        fi
        
        unzip ${LATEST_BINARY} -d ${DIRECTORY}
        rm ${LATEST_BINARY}

        echo ${LATEST_VERSION} > local_version.txt

        if [ -f "appsettings.json" ]
        then
            mv appsettings.json ${DIRECTORY}/appsettings.json
        fi

        chmod a+x ./${DIRECTORY}/dexih.remote

        PREVIOUS_BINARY=${LATEST_BINARY}
    fi

    if [ ! -f "${DIRECTORY}/dexih.remote" ]; then
        echo The dexih.remote binary was not found in the directory ${DIRECTORY}.
        exit
    fi

    # Run the remote agent
    pushd ${DIRECTORY}
        ./dexih.remote $1
        UPGRADE=$?
    popd
done
