$version = [IO.File]::ReadAllText(".\version.txt")
Write-Host 'Old version' $version
[int]$intVersion = [convert]::ToInt32($version, 10)
$intVersion = $intVersion + 1;
$version = $intVersion.ToString('00000');
Write-Host 'New version' $version

dotnet restore -r win7-x64
dotnet publish -c release -r win7-x64 -f net462 --version-suffix C"$version"
# dotnet restore -r win7-x64 -f netcoreapp2.0
dotnet publish -c release -r win7-x64 -f netcoreapp2.0 --version-suffix C"$version"
dotnet restore -r osx-x64
dotnet publish -c release -r osx-x64 -f netcoreapp2.0 --version-suffix C"$version"
dotnet restore -r linux-x64
dotnet publish -c release -r linux-x64 -f netcoreapp2.0 --version-suffix C"$version"

# if([System.IO.File]::Exists('..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.zip')){ Remove-Item ..\dexih.application\wwwroot\remote.binaries\dexih.remote.windows.zip }
if([System.IO.File]::Exists('..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.netcore.zip')){ Remove-Item ..\dexih.application\wwwroot\remote.binaries\dexih.remote.windows.netcore.zip }
if([System.IO.File]::Exists('..\dexih.api\wwwroot\remote.binaries\dexih.remote.mac.zip')){ Remove-Item ..\dexih.application\wwwroot\remote.binaries\dexih.remote.mac.zip }
if([System.IO.File]::Exists('..\dexih.api\wwwroot\remote.binaries\dexih.remote.linux.zip')){ Remove-Item ..\dexih.application\wwwroot\remote.binaries\dexih.remote.linux.zip }

#Add-Type -A System.IO.Compression.FileSystem
#[IO.Compression.ZipFile]::CreateFromDirectory('bin\release\net46\win7-x64\publish\', '..\dexih.application\wwwroot\remote.binaries\dexih.remote.windows.zip')
#[IO.Compression.ZipFile]::CreateFromDirectory('bin\release\netcoreapp1.0\\win7-x64\publish\', '..\dexih.application\wwwroot\remote.binaries\dexih.remote.windows.netcore.zip')
#[IO.Compression.ZipFile]::CreateFromDirectory('bin\release\netcoreapp1.0\osx-x64\publish\', '..\dexih.application\wwwroot\remote.binaries\dexih.remote.mac.zip')
#[IO.Compression.ZipFile]::CreateFromDirectory('bin\release\netcoreapp1.0\linux-x64\publish\', '..\dexih.application\wwwroot\remote.binaries\dexih.remote.linux.zip')

& ".\7za.exe" a ..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.zip ./bin/release/net462/win10-x64/publish/*
& ".\7za.exe" a ..\dexih.api\wwwroot\remote.binaries\dexih.remote.windows.netcore.zip ./bin/release/netcoreapp2.0/win7-x64/publish/*
& ".\7za.exe" a ..\dexih.api\wwwroot\remote.binaries\dexih.remote.mac.zip ./bin/release/netcoreapp2.0/osx-x64/publish/*
& ".\7za.exe" a ..\dexih.api\wwwroot\remote.binaries\dexih.remote.linux.zip ./bin/release/netcoreapp2.0/linux-x64/publish/*
