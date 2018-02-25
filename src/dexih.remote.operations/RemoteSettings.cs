using System.Net.Sockets;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Sockets;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.operations
{
    
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EPrivacyLevel
    {
        LockData = 1,
        AllowDataUpload = 2,
        AllowDataDownload = 3
    }

    /// <summary>
    /// Class mapping of the AppSettings file used for the RemoteAgent settings.
    /// </summary>
    public class RemoteSettings
    {
        public AppSettingsSection AppSettings { get; set; } = new AppSettingsSection();
        public SystemSettingsSection SystemSettings { get; set; } = new SystemSettingsSection();
        public LoggingSection Logging { get; set; } = new LoggingSection();
    }
    
    public class AppSettingsSection
    {

        public string User { get; set; }
        public string UserToken { get; set; }
        public string EncryptionKey { get; set; }
        public string WebServer { get; set; }
        public string Name { get; set; }
        public bool AutoSchedules { get; set; }
        public bool AutoUpgrade { get; set; }
        public bool PreRelease { get; set; } = false;
        public bool AllowAzureStorage { get; set; }
        public EPrivacyLevel PrivacyLevel { get; set; } = EPrivacyLevel.AllowDataDownload;
        public string LocalDataSaveLocation { get; set; }
        public string RemoteAgentId { get; set; }
        public string AgentSecret { get; set; }
        public int DownloadPort { get; set; }
        public string ExternalDownloadUrl { get; set; }
        public long MaxUploadSize { get; set; }
        
        [JsonIgnore]
        public string Password { get; set; }
        
        [JsonIgnore]
        public string IpAddress { get; set; }
    }

    public class SystemSettingsSection
    {
        public int MaxAcknowledgeWait { get; set; } = 5000;
        public int ResponseTimeout { get; set; } = 1000000;
        public int CancelDelay { get; set; } = 1000;
        public int EncryptionIteractions { get; set; } = 1000;
        public int MaxPreviewDuration { get; set; } = 10000;
        public int MaxConcurrentTasks { get; set; } = 50;
        public TransportType SocketTransportType { get; set; } = TransportType.WebSockets;
    }

    public class LoggingSection
    {
        public bool IncludeScopes { get; set; } = false;
        public LogLevelSection LogLevel { get; set; } = new LogLevelSection();
    }

    public class LogLevelSection
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel Default { get; set; } = LogLevel.Information;
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel System { get; set; } = LogLevel.Information;
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel Microsoft { get; set; } = LogLevel.Information;
    }
}