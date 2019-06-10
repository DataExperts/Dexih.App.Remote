#!/usr/bin/env bash
PASSWORD=$1

PRERELEASE='false'

# Get the program version (i.e. v0.4.0)
VERSION_PREFIX=`more version.txt | awk '{print $1}'`

# Get the build version
VERSION_SUFFIX=`more version.txt | awk '{print $2}'`

VERSION=${VERSION_PREFIX}-${VERSION_SUFFIX}
echo $VERSION

# Upload the release to github releases
RESPONSE=$(curl https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases -d '{"tag_name": "'v${VERSION}'", "target_commitish": "master", "name": "Release version 'v${VERSION}'", "body": "Release notes not available for this release", "draft": false, "prerelease": '${PRERELEASE}' }' -u gholland@dataexpertsgroup.com:$PASSWORD )
URL=$(echo $RESPONSE | jq -r '.upload_url')
URL=${URL//"{?name,label}"/}
echo UPLOAD URL - $URL

echo UPLOAD LINUX BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.linux_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.linux${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl --progress-bar  -o upload.txt -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"

echo UPLOAD OSX BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.osx_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.osx_${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl --progress-bar  -o upload.txt -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"

echo UPLOAD WINDOWS BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.windows_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.windows_${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl --progress-bar  -o upload.txt -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"

echo UPLOAD ALPINE BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.alpine_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.alpine_${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl --progress-bar  -o upload.txt -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"

rm upload.txt

echo UPLOAD DOCKER IMAGE
docker push dexih/remote:${VERSION_PREFIX}-${VERSION_SUFFIX}
