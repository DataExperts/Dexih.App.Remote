#!/usr/bin/env bash
# This script downloads the remote agent binaries and runs the remote agent.

SERVER='{{SERVER}}'
DIRECTORY='remote.agent'
OS='linux'
PRE='{{PRE}}'

SCRIPT=`basename "$0"`

# Install unzip if is does not exist
if ! [ -x "$(command -v unzip)" ]; then
    apt-get update
    apt-get -q -y install unzip
fi

# Install unzip if is does not exist
if ! [ -x "$(command -v curl)" ]; then
    apt-get update
    apt-get -q -y install curl
fi

if [ ! -d "${DIRECTORY}" ]; then
    mkdir ${DIRECTORY}
fi

# create a start script if it doesn't exist
if [ ! -f "start.sh" ]; then
    echo "./${SCRIPT} > log\`date +"%Y%m%d_%H%M%S"\`.txt &" > start.sh
    chmod a+x start.sh
    echo "***** Use start.sh to run the process in the background with logs. *****"
fi

# create a stop script if it doesn't exist
if [ ! -f "stop.sh" ]; then
    echo "sudo pkill -f dexih.remote" > stop.sh
    chmod a+x stop.sh
    echo "***** Use stop.sh to stop the background agent. *****"
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
        LOCAL_VERSION=`cat local_version.txt`
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

#        # back up the appsettings
#        if [ -f "${DIRECTORY}/appsettings.json" ]
#        then
#            mv ${DIRECTORY}/appsettings.json .
#        fi
#        
#        # back up the pfx files  
#        if [ -f "${DIRECTORY}/*.pfx" ]
#        then
#            mv ${DIRECTORY}/*.pfx .
#        fi

        # Clean out the previous directory contents.
        if [ -d ${DIRECTORY} ]; then
            rm -rf ${DIRECTORY}/*
        else
            mkdir ${DIRECTORY}
        fi
        
        unzip -q ${LATEST_BINARY} -d ${DIRECTORY}
        rm ${LATEST_BINARY}

        echo ${LATEST_VERSION} > local_version.txt

#        if [ -f "appsettings.json" ]
#        then
#            mv appsettings.json ${DIRECTORY}/appsettings.json
#        fi
#        
#        if [ -f "*.pfx" ]
#        then
#            mv *.pfx ${DIRECTORY}/
#        fi

        chmod a+x ./${DIRECTORY}/dexih.remote

        PREVIOUS_BINARY=${LATEST_BINARY}
        PREVIOUS_VERSION=${LATEST_VERSION}
    fi

    if [ ! -f "${DIRECTORY}/dexih.remote" ]; then
        echo The dexih.remote binary was not found in the directory ${DIRECTORY}.
        exit
    fi

    # Run the remote agent
    ${DIRECTORY}/dexih.remote $1
    UPGRADE=$?
done
