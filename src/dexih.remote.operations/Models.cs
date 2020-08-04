using System.Collections.Generic;
using System.Runtime.Serialization;
using dexih.operations;
using dexih.repository;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;


namespace dexih.remote.operations
{
    [DataContract]
    public class DownloadReadyBase
    {
        [DataMember(Order = 0)]
        public string InstanceId { get; set; }

        [DataMember(Order = 1)]
        public string SecurityToken { get; set; }
    
        [DataMember(Order = 2)]
        public string ConnectionId { get; set; }
    
        [DataMember(Order = 3)]
        public string Reference { get; set; }
    
        [DataMember(Order = 4)]
        public long HubKey { get; set; }
    }

    [DataContract]
    public class DownloadReadyMessage: DownloadReadyBase
    {
        [DataMember(Order = 5)]
        public string Url { get; set; }
    }

    public class FlatFilesReadyMessage: DownloadReadyBase
    {
        [DataMember(Order = 5)]
        public DexihTable[] Tables { get; set; }
        
        [DataMember(Order = 6)]
        public ReturnValue Message { get; set; }
    }

    [DataContract]
    public class RenewSslCertificateModel
    {
        [DataMember(Order = 0)]
        public string Domain { get; set; }

        [DataMember(Order = 1)]
        public string Password { get; set; }
    }

    [DataContract]
    public class DatalinkProgress
    {
        [DataMember(Order = 0)]
        public string InstanceId { get; set; }

        [DataMember(Order = 2)]
        public string SecurityToken { get; set; }
        
        [DataMember(Order = 3)]
        public EMessageCommand Command { get; set; }
        
        [DataMember(Order = 4)]
        public List<ManagedTask> Results { get; set; } 
    }

    
    // class contains data required when program exits
    public class ProgramExit
    {
        public bool CompleteUpgrade { get; set; } = false;
    }
   
}