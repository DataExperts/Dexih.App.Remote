#!/usr/bin/env bash

# Get the program version (i.e. v0.4.0)
VERSION_PREFIX=`more version.txt | awk '{print $1}'`

# Get the build version
VERSION_SUFFIX=`more version.txt | awk '{print $2}' | sed 's/^0*//'`

# Add 1 to the build version, and write back to version.txt
VERSION_SUFFIX=`printf "%05d\n" $((VERSION_SUFFIX +1))`
echo 'Building version: ' $VERSION_PREFIX $VERSION_SUFFIX
printf "%s %s" ${VERSION_PREFIX} ${VERSION_SUFFIX} > version.txt

dotnet restore
# dotnet publish -c release -r win7-x64 -f net46
dotnet publish -c release -r win7-x64 -f netcoreapp2.2 --version-suffix ${VERSION_SUFFIX} -p:VersionPrefix=${VERSION_PREFIX}
dotnet publish -c release -r osx-x64 -f netcoreapp2.2 --version-suffix ${VERSION_SUFFIX} -p:VersionPrefix=${VERSION_PREFIX}
dotnet publish -c release -r linux-x64 -f netcoreapp2.2 --version-suffix ${VERSION_SUFFIX} -p:VersionPrefix=${VERSION_PREFIX}
dotnet publish -c release -r alpine.3.6-x64 -f netcoreapp2.2 --version-suffix ${VERSION_SUFFIX} -p:VersionPrefix=${VERSION_PREFIX}

pushd ./bin/release/netcoreapp2.2/win7-x64/publish
zip -qr ../../../../../releases/dexih.remote.windows_${VERSION_PREFIX}-${VERSION_SUFFIX}.zip *
popd

pushd ./bin/release/netcoreapp2.2/osx-x64/publish
chmod a+x dexih.remote
zip -qr ../../../../../releases/dexih.remote.osx_${VERSION_PREFIX}-${VERSION_SUFFIX}.zip *
popd

pushd ./bin/release/netcoreapp2.2/linux-x64/publish
chmod a+x dexih.remote
zip -qr ../../../../../releases/dexih.remote.linux_${VERSION_PREFIX}-${VERSION_SUFFIX}.zip *
popd

pushd ./bin/release/netcoreapp2.2/alpine.3.6-x64/publish
chmod a+x dexih.remote
zip -qr ../../../../../releases/dexih.remote.alpine_${VERSION_PREFIX}-${VERSION_SUFFIX}.zip *
popd

docker build . -t dexih/remote:latest -t dexih/remote:${VERSION_PREFIX}-${VERSION_SUFFIX} --build-arg RELEASE=releases/dexih.remote.alpine_${VERSION_PREFIX}-${VERSION_SUFFIX}.zip