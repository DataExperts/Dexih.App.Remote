FROM microsoft/dotnet:2.1-runtime

WORKDIR /app
# ADD AppSettinss.json /app/
ADD scripts/dexih.remote.run.linux.sh /app/
# ADD bin/release/netcoreapp2.1/linux-x64/publish /app/remote.agent

EXPOSE 33944:33944

ENV "AppSettings:FirstRun"=false
ENV "AppSettings:RemoteAgentId"=
ENV "AppSettings:User"=
ENV "AppSettings:UserToken"=
ENV "AppSettings:EncryptionKey"=
ENV "AppSettings:WebServer"="https://dexih.dataexpertsgroup.com"
ENV "AppSettings:Name"=
ENV "AppSettings:AutoUpgrade"=true
ENV "AppSettings:AllowPreReleases"=false
ENV "AppSettings:AllowAzureStorage"=false
ENV "AppSettings:AllowDataDownload"=true
ENV "AppSettings:AllowDataUpload"=true
ENV "AppSettings:DownloadDirectly"=true
ENV "AppSettings:DownloadPort"=33944
ENV "AppSettings:ExternalDownloadUrl"=
ENV "AppSettings:AllowLocalFiles"=true
ENV "AppSettings:AllowAllPaths"=true
ENV "AppSettings:AllowedPaths:0"=
ENV "AppSettings:AllowAllHubs"=true
ENV "AppSettings:AllowedHubs:0"=
ENV "AppSettings:MaxUploadSize"=1000000000
ENV "AppSettings:EnforceHttps"=true
ENV "AppSettings:AutoGenerateCertificate"=true
ENV "AppSettings:DynamicDomain"=true
ENV "AppSettings:CertificateFilename"=
ENV "AppSettings:CertificatePassword"=
ENV "AppSettings:DownloadLocalIp"=false

ENV "SystemSettings:MaxAcknowledgeWait"=5000
ENV "SystemSettings:ResponseTimeout"=1000000
ENV "SystemSettings:CancelDelay"=1000
ENV "SystemSettings:EncryptionIteractions"=1000
ENV "SystemSettings:MaxPreviewDuration"=10000
ENV "SystemSettings:MaxConcurrentTasks"=50
ENV "SystemSettings:SocketTransportType"=1

ENV "Logging:IncludeScopes"=false
ENV "Logging:LogLevel:Default"="Information"
ENV "Logging:LogLevel:System"="Information"
ENV "Logging:LogLevel:Microsoft"="Information"

ENTRYPOINT ["/app/dexih.remote.run.linux.sh"]  
