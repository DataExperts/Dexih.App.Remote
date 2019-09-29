using dexih.repository;
using MessagePack;

namespace dexih.remote.operations
{
    [MessagePackObject]
    public class DownloadReadyBase
    {
        [Key(0)]
        public string InstanceId { get; set; }

        [Key(1)]
        public string SecurityToken { get; set; }
    
        [Key(2)]
        public string ConnectionId { get; set; }
    
        [Key(3)]
        public string Reference { get; set; }
    
        [Key(4)]
        public long HubKey { get; set; }
    }

    [MessagePackObject]
    public class DownloadReadyMessage: DownloadReadyBase
    {
        [Key(5)]
        public string Url { get; set; }
    }

    public class FlatFilesReadyMessage: DownloadReadyBase
    {
        [Key(5)]
        public DexihTable[] Tables { get; set; }
    
    }

    [MessagePackObject]
    public class RenewSslCertificateModel
    {
        [Key(0)]
        public string Domain { get; set; }

        [Key(1)]
        public string Password { get; set; }
    }

   
}