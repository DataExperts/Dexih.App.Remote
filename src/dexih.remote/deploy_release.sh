#!/usr/bin/env bash
PASSWORD=$1
VERSION=`more version.txt | sed 's/^0*//'`
VERSION=`printf "%05d\n" $((VERSION))`
echo $VERSION

# Upload the release to github releases
RESPONSE=$(curl https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases -d '{"tag_name": "'${VERSION}'", "target_commitish": "master", "name": "Release version '${VERSION}'", "body": "Release version '${VERSION}'", "draft": false, "prerelease": false }' -u gholland@dataexpertsgroup.com:$PASSWORD )
URL=$(echo $RESPONSE | jq -r '.upload_url')
URL=${URL//"{?name,label}"/}
echo UPLOAD URL - $URL

echo UPLOAD OSX BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.osx_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.osx_${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"

echo UPLOAD LINUX BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.linux_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.linux${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"

echo UPLOAD WINDOWS BINARY
FILE=/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/releases/dexih.remote.windows_${VERSION}.zip
UPLOADURL="${URL}?name=dexih.remote.windows_${VERSION}.zip"
echo FILE - ${FILE}
echo UPLOADURL - ${UPLOADURL}
curl -u gholland@dataexpertsgroup.com:$PASSWORD  -H "Content-Type: application/octet-stream" --data-binary @"${FILE}" "${UPLOADURL}"
