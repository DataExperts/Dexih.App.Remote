using System.Collections.Generic;
using dexih.operations;
using Dexih.Utils.ManagedTasks;
using MessagePack;

namespace dexih.remote.operations
{
    [MessagePackObject]
    public class RemoteAgentStatus
    {
        [Key(0)]
        public IEnumerable<ApiData> ActiveApis { get; set; }

        [Key(1)]
        public IEnumerable<ManagedTask> ActiveDatajobs { get; set; }
        
        [Key(2)]
        public IEnumerable<ManagedTask> ActiveDatalinks { get; set; }
        
        [Key(3)]
        public IEnumerable<ManagedTask> ActiveDatalinkTests { get; set; }
        
        [Key(4)]
        public IEnumerable<ManagedTask> PreviousDatajobs { get; set; }
        
        [Key(5)]
        public IEnumerable<ManagedTask> PreviousDatalinks { get; set; }
        
        [Key(6)]
        public IEnumerable<ManagedTask> PreviousDatalinkTests { get; set; }
        
        [Key(7)]
        public RemoteLibraries RemoteLibraries { get; set; }
        
        [Key(8)]

        public bool RequiresUpgrade { get; set; }
    }
}