#!/usr/bin/env bash
# This script downloads the remote agent binaries and runs the remote agent.

DIRECTORY='remote.agent'

if [ ! -d "${DIRECTORY}" ]; then
    mkdir ${DIRECTORY}
fi

# 20 is the return value of the dexih.remote if an upgrade is suggested.
UPGRADE=20

while [ ${UPGRADE} -eq 20 ]
do
    GIT_DATA=`curl -s "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases/latest"`
    LATEST_RELEASE_BINARY=`echo $GIT_DATA |  jq -r '.assets[].name' | grep osx`
    LATEST_RELEASE_URL=`echo $GIT_DATA |  jq -r '.assets[].browser_download_url' | grep osx`

    if [ -f "local_binary.txt" ]; then
        LOCAL_BINARY=`more local_binary.txt`
    fi

    echo LOCAL_VERSION ${LOCAL_VERSION}
    echo LOCAL_BUILD ${LOCAL_BUILD}
    echo LATEST_RELEASE_BINARY ${LATEST_RELEASE_BINARY}
    echo LATEST_RELEASE_URL ${LATEST_RELEASE_URL}

    if [ ! -z "${PREVIOUS_BINARY}" ] && [ "${PREVIOUS_BINARY}" -eq "${LATEST_RELEASE_BINARY}" ]; then
        echo The latest binary has already been downloaded, and another upgrade has been requested.
        echo The script will exit to avoid a infinte download loop.
        exit
    fi
    
    if [ ! -z "${LATEST_RELEASE_URL}" ]; then
        if [ "${LATEST_RELEASE_BINARY}" != "${LOCAL_BINARY}" ]  || [ ! -d ${DIRECTORY}  ]; then
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

            # Clean out the previous directory contents.
            if [ -d ${DIRECTORY} ]; then
                rm -rf ${DIRECTORY}/*
            else
                mkdir ${DIRECTORY}
            fi
            
            unzip ${LATEST_RELEASE_BINARY} -d ${DIRECTORY}
            rm ${LATEST_RELEASE_BINARY}

            echo $LATEST_RELEASE_BINARY > local_binary.txt

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
    pushd ${DIRECTORY}
        ./dexih.remote $1
        UPGRADE=$?
    popd
done
