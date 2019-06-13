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

# ENV "DEXIH_AppSettings__UserPrompt"=false
# ENV "DEXIH_AppSettings__RemoteAgentId"=
# ENV "DEXIH_AppSettings__User"=
# ENV "DEXIH_AppSettings__UserToken"=
# ENV "DEXIH_AppSettings__EncryptionKey"=
# ENV "DEXIH_AppSettings__WebServer"="https://dexih.dataexpertsgroup.com"
# ENV "DEXIH_AppSettings__Name"=
# ENV "DEXIH_AppSettings__AutoUpgrade"=true
# ENV "DEXIH_AppSettings__AllowPreReleases"=false

# ENV "DEXIH_Permissions__AllowLocalFiles"=true
ENV "DEXIH_Permissions__AllowAllPaths"=false
ENV "DEXIH_Permissions__AllowedPaths__0"="/data"
# ENV "DEXIH_Permissions__AllowAllHubs"=true
# ENV "DEXIH_Permissions__AllowedHubs__0"=

ENV "DEXIH_Network__LocalDownloadIp"=
ENV "DEXIH_Network__LocalDownloadPort"=
# ENV "DEXIH_Network__ExternalDownloadUrl"=
# ENV "DEXIH_Network__ProxyUrl"=
# ENV "DEXIH_Network__DownloadPort"=33944
# ENV "DEXIH_Network__EnforceHttps"=true
# ENV "DEXIH_Network__AutoGenerateCertificate"=true
# ENV "DEXIH_Network__DynamicDomain"=true
# ENV "DEXIH_Network__CertificateFilename"=
# ENV "DEXIH_Network__CertificatePassword"=
# ENV "DEXIH_Network__EnableUPnP"=

# ENV "DEXIH_Privacy__AllowDataDownload"=true
# ENV "DEXIH_Privacy__AllowDataUpload"=true
# ENV "DEXIH_Privacy__AllowLanAccess"=true
# ENV "DEXIH_Privacy__AllowExternalAccess"=true
# ENV "DEXIH_Privacy__AllowProxy"=true

# ENV "DEXIH_SystemSettings__MaxAcknowledgeWait"=5000
# ENV "DEXIH_SystemSettings__ResponseTimeout"=1000000
# ENV "DEXIH_SystemSettings__CancelDelay"=1000
# ENV "DEXIH_SystemSettings__EncryptionIteractions"=1000
# ENV "DEXIH_SystemSettings__MaxPreviewDuration"=10000
# ENV "DEXIH_SystemSettings__MaxConcurrentTasks"=50
# ENV "DEXIH_SystemSettings__MaxUploadSize"=1000000000
# ENV "DEXIH_SystemSettings__SocketTransportType"=1

# ENV "DEXIH_Logging__IncludeScopes"=false
# ENV "DEXIH_Logging__LogLevel__Default"="Information"
# ENV "DEXIH_Logging__LogLevel__System"="Information"
# ENV "DEXIH_Logging__LogLevel__Microsoft"="Information"
ENV "DEXIH_Logging__LogFilePath"="/log"


VOLUME /data /config /logs
WORKDIR /app
ENTRYPOINT ["/app/dexih.remote.run.alpine.sh"]  
