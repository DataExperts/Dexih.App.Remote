#!/bin/sh
# This script downloads the remote agent binaries and runs the remote agent.

DIRECTORY='remote.agent'

if [ ! -d "${DIRECTORY}" ]; then
    mkdir ${DIRECTORY}
fi

if [ -f "latest_version.txt" ]; then
    mv latest_version.txt ${DIRECTORY}
fi

# 20 is the return value of the dexih.remote if an upgrade is suggested.
UPGRADE=20

while [ ${UPGRADE} -eq 20 ]
do

    if [ -f "${DIRECTORY}/latest_version.txt" ]; then
    # First line contain the version
        LATEST_RELEASE_VERSION=`more ${DIRECTORY}/latest_version.txt | head -1`
        LATEST_RELEASE_BUILD=`echo "${LATEST_RELEASE_VERSION}" | rev | cut -d "-" -f 1 | rev`
    # Second line contains the download url
        LATEST_RELEASE_URL=`more ${DIRECTORY}/latest_version.txt | head -2 | tail -1`
        LATEST_RELEASE_BINARY=`echo $(basename "${LATEST_RELEASE_URL}")`
    fi


    if [ -f "${DIRECTORY}/version.txt" ]; then
        LOCAL_VERSION=`more ${DIRECTORY}/version.txt`
        LOCAL_BUILD=`more ${DIRECTORY}/version.txt | rev | cut -d " " -f 1 | rev`
    fi

    echo LOCAL_VERSION ${LOCAL_VERSION}
    echo LOCAL_BUILD ${LOCAL_BUILD}
    echo LATEST_RELEASE_BINARY ${LATEST_RELEASE_BINARY}
    echo LATEST_RELEASE_VERSION ${LATEST_RELEASE_VERSION}
    echo LATEST_RELEASE_BUILD ${LATEST_RELEASE_BUILD}
    echo LATEST_RELEASE_URL ${LATEST_RELEASE_URL}

    if [ ! -z "${PREVIOUS_BINARY}" ] && [ "${PREVIOUS_BINARY}" -eq "${LATEST_RELEASE_BINARY}" ]; then
        echo The latest binary has already been downloaded, and another upgrade has been requested.
        echo The script will exit to avoid a infinte download loop.
        exit
    fi
    
    if [ ! -z "${LATEST_RELEASE_URL}" ]; then
        if [ "${LATEST_RELEASE_BUILD}" -ne "${LOCAL_BUILD}" ]  || [ ! -d ${DIRECTORY}  ]; then
            # Install curl if it does not exist
            if ! [ -x "$(command -v curl)" ]; then
                apk update
                apk add curl
            fi

            curl -L -o "${LATEST_RELEASE_BINARY}" "${LATEST_RELEASE_URL}"

            if [ ! -f ${LATEST_RELEASE_BINARY} ]; then
                echo The file ${LATEST_RELEASE_BINARY} was not downloaded.
                exit
            fi

            # back up the appsettings
            if [ -f "${DIRECTORY}/appsettings.json" ]
            then
                mv ${DIRECTORY}/appsettings.json .
            fi

            # Install unzip if it does not exist
            if ! [ -x "$(command -v unzip)" ]; then
                apk update
                apk add unzip
            fi

            # Clean out the previous directory contents.
            if [ -d ${DIRECTORY} ]; then
                rm -rf ${DIRECTORY}/*
            else
                mkdir ${DIRECTORY}
            fi
            
            unzip ${LATEST_RELEASE_BINARY} -d ${DIRECTORY}
            rm ${LATEST_RELEASE_BINARY}

            if [ -f "appsettings.json" ]
            then
                mv appsettings.json ${DIRECTORY}/appsettings.json
            fi

            chmod a+x ./${DIRECTORY}/dexih.remote

            PREVIOUS_BINARY=${LATEST_RELEASE_BINARY}
        fi
    fi

    if [ ! -f "${DIRECTORY}/dexih.remote" ]; then
        echo The dexih.remote binary was not found in the directory ${DIRECTORY}.
        exit
    fi

    # Run the remote agent
    oldpath=`pwd`
    cd ${DIRECTORY}
    ./dexih.remote $1
    UPGRADE=$?
    cd $oldpath
done
