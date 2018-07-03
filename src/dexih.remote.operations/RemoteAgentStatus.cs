using System.Collections.Generic;
using dexih.operations;
using Dexih.Utils.ManagedTasks;

namespace dexih.remote.operations
{
    public class RemoteAgentStatus
    {
        public IEnumerable<ManagedTask> ActiveDatajobs { get; set; }
        public IEnumerable<ManagedTask> ActiveDatalinks { get; set; }
        public IEnumerable<ManagedTask> PreviousDatajobs { get; set; }
        public IEnumerable<ManagedTask> PreviousDatalinks { get; set; }
        public RemoteLibraries RemoteLibraries { get; set; }
    }
}