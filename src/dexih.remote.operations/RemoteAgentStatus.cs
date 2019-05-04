using System.Collections.Generic;
using dexih.operations;
using dexih.remote.Operations.Services;
using Dexih.Utils.ManagedTasks;

namespace dexih.remote.operations
{
    public class RemoteAgentStatus
    {
        public IEnumerable<ApiData> ActiveApis { get; set; }
        public IEnumerable<ManagedTask> ActiveDatajobs { get; set; }
        public IEnumerable<ManagedTask> ActiveDatalinks { get; set; }
        public IEnumerable<ManagedTask> ActiveDatalinkTests { get; set; }
        public IEnumerable<ManagedTask> PreviousDatajobs { get; set; }
        public IEnumerable<ManagedTask> PreviousDatalinks { get; set; }
        public IEnumerable<ManagedTask> PreviousDatalinkTests { get; set; }
        public RemoteLibraries RemoteLibraries { get; set; }
        
        public bool RequiresUpgrade { get; set; }
    }
}