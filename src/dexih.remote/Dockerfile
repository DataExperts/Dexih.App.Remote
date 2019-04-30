ARG REPO=microsoft/dotnet
FROM $REPO:2.2-runtime-deps-alpine3.8

# Install .NET Core
ENV DOTNET_VERSION 2.2.1

ARG RELEASE

RUN wget -O dotnet.tar.gz https://dotnetcli.blob.core.windows.net/dotnet/Runtime/$DOTNET_VERSION/dotnet-runtime-$DOTNET_VERSION-linux-musl-x64.tar.gz \
    && dotnet_sha512='f76f8650aae126b2cdf55cce200d6400137288f5c0688e314575273ab9f87f364d06dcf0992524d8d6a127485ec11f8f6e9586a5b47604bf1f6396748b3e7fca' \
    && echo "$dotnet_sha512  dotnet.tar.gz" | sha512sum -c - \
    && mkdir -p /usr/share/dotnet \
    && tar -C /usr/share/dotnet -xzf dotnet.tar.gz \
    && ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet \
    && rm dotnet.tar.gz

COPY ${RELEASE}* /app/

# Install dexih.remote
RUN mkdir -p /app \
    && wget -O /app/dexih.remote.run.alpine.sh https://dexih.dataexpertsgroup.com/api/Remote/DownloadRemoteRun/alpine/stable \
    && chmod a+x /app/dexih.remote.run.alpine.sh

EXPOSE 33944:33944

ENV DEXIH_CONFIG_DIRECTORY="/config"

# ENV "AppSettings__UserPrompt"=false
# ENV "AppSettings__RemoteAgentId"=
# ENV "AppSettings__User"=
# ENV "AppSettings__UserToken"=
# ENV "AppSettings__EncryptionKey"=
# ENV "AppSettings__WebServer"="https://dexih.dataexpertsgroup.com"
# ENV "AppSettings__Name"=
# ENV "AppSettings__AutoUpgrade"=true
# ENV "AppSettings__AllowPreReleases"=false

# ENV "Permissions__AllowLocalFiles"=true
ENV "Permissions__AllowAllPaths"=false
ENV "Permissions__AllowedPaths__0"="/data"
# ENV "Permissions__AllowAllHubs"=true
# ENV "Permissions__AllowedHubs__0"=

ENV "Network__LocalDownloadIp"=
ENV "Network__LocalDownloadPort"=
# ENV "Network__ExternalDownloadUrl"=
# ENV "Network__ProxyUrl"=
# ENV "Network__DownloadPort"=33944
# ENV "Network__EnforceHttps"=true
# ENV "Network__AutoGenerateCertificate"=true
# ENV "Network__DynamicDomain"=true
# ENV "Network__CertificateFilename"=
# ENV "Network__CertificatePassword"=
# ENV "Network__EnableUPnP"=

# ENV "Privacy__AllowDataDownload"=true
# ENV "Privacy__AllowDataUpload"=true
# ENV "Privacy__AllowLanAccess"=true
# ENV "Privacy__AllowExternalAccess"=true
# ENV "Privacy__AllowProxy"=true

# ENV "SystemSettings__MaxAcknowledgeWait"=5000
# ENV "SystemSettings__ResponseTimeout"=1000000
# ENV "SystemSettings__CancelDelay"=1000
# ENV "SystemSettings__EncryptionIteractions"=1000
# ENV "SystemSettings__MaxPreviewDuration"=10000
# ENV "SystemSettings__MaxConcurrentTasks"=50
# ENV "SystemSettings__MaxUploadSize"=1000000000
# ENV "SystemSettings__SocketTransportType"=1

# ENV "Logging__IncludeScopes"=false
# ENV "Logging__LogLevel__Default"="Information"
# ENV "Logging__LogLevel__System"="Information"
# ENV "Logging__LogLevel__Microsoft"="Information"
ENV "Logging__LogFilePath"="/log"


VOLUME /data /config /logs
WORKDIR /app
ENTRYPOINT ["/app/dexih.remote.run.alpine.sh"]  
