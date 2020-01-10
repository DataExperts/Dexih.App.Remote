using System.Collections.Generic;
using System.Runtime.Serialization;
using dexih.operations;
using Dexih.Utils.ManagedTasks;


namespace dexih.remote.operations
{
    [DataContract]
    public class RemoteAgentStatus
    {
        [DataMember(Order = 0)]
        public IEnumerable<ApiData> ActiveApis { get; set; }

        [DataMember(Order = 1)]
        public IEnumerable<ManagedTask> ActiveDatajobs { get; set; }
        
        [DataMember(Order = 2)]
        public IEnumerable<ManagedTask> ActiveDatalinks { get; set; }
        
        [DataMember(Order = 3)]
        public IEnumerable<ManagedTask> ActiveDatalinkTests { get; set; }
        
        [DataMember(Order = 4)]
        public IEnumerable<ManagedTask> PreviousDatajobs { get; set; }
        
        [DataMember(Order = 5)]
        public IEnumerable<ManagedTask> PreviousDatalinks { get; set; }
        
        [DataMember(Order = 6)]
        public IEnumerable<ManagedTask> PreviousDatalinkTests { get; set; }
        
        [DataMember(Order = 7)]
        public RemoteLibraries RemoteLibraries { get; set; }
        
        [DataMember(Order = 8)]

        public bool RequiresUpgrade { get; set; }
    }
}