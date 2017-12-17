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

if [ ! -d ../dexih.api/wwwroot/remote.binaries ]; 
then
 mkdir ../dexih.api/wwwroot/remote.binaries
fi

# if [ -f ..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.zip ]; 
# then
#  rm ..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.zip
# fi

if [ -f ..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.netcore.zip ]; 
then
 rm ..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.netcore.zip
fi

if [ -f ..\dexih.api\wwwroot\remote.binaries\dexih.remote.mac.zip ]; 
then
 rm ..\dexih.api\wwwroot\remote.binaries\ddexih.remote.mac.zip
fi

if [ -f ..\dexih.api\wwwroot\remote.binaries\dexih.remote.linux.zip ]; 
then
 rm ..\dexih.api\wwwroot\remote.binaries\dexih.remote.linux.zip
fi

# pushd ./bin/release/net46/win7-x64/publish
# zip -r ../../../../../../dexih.api/Resources/RemoteAgents/dexih.remote.windows.zip *
# popd

pushd ./bin/release/netcoreapp2.0/win7-x64/publish
zip -r ../../../../../../dexih.api/wwwroot/remote.binaries/dexih.remote.windows.netcore.zip *
popd

pushd ./bin/release/netcoreapp2.0/osx-x64/publish
zip -r ../../../../../../dexih.api/wwwroot/remote.binaries/dexih.remote.mac.zip *
popd

pushd ./bin/release/netcoreapp2.0/linux-x64/publish
zip -r ../../../../../../dexih.api/wwwroot/remote.binaries/dexih.remote.linux.zip *
popd

cp version.txt ../dexih.api/wwwroot/remote.binaries