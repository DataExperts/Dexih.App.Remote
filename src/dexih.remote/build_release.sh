#!/usr/bin/env bash

# Add a new version and save.
VERSION=`more version.txt | sed 's/^0*//'`
echo OLD VERSION=$VERSION
VERSION=`printf "%05d\n" $((VERSION +1))`
echo NEW VERSION=$VERSION
printf $VERSION > version.txt

dotnet restore
# dotnet publish -c release -r win7-x64 -f net46
#dotnet restore -r win7-x64 
dotnet publish -c release -r win7-x64 -f netcoreapp2.0 --version-suffix C$VERSION
#dotnet restore -r osx-x64
dotnet publish -c release -r osx-x64 -f netcoreapp2.0 --version-suffix C$VERSION
#dotnet restore -r linux-x64
dotnet publish -c release -r linux-x64 -f netcoreapp2.0 --version-suffix C$VERSION

pushd ./bin/release/netcoreapp2.0/win7-x64/publish
zip -r ../../../../../releases/dexih.remote.windows_${VERSION}.zip *
popd

pushd ./bin/release/netcoreapp2.0/osx-x64/publish
zip -r ../../../../../releases/dexih.remote.osx_${VERSION}.zip *
popd

pushd ./bin/release/netcoreapp2.0/linux-x64/publish
zip -r ../../../../../releases/dexih.remote.linux_${VERSION}.zip *
popd

