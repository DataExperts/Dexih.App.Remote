using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    // Wrapper for manage tasks that supports dependency injection for logger
    public class ManagedTasksService: ManagedTasks
    {
        public ManagedTasksService(ILogger<ManagedTasks> logger): base(100, logger)
        {
        }
    }
}