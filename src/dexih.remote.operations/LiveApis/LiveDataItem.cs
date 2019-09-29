using dexih.transforms;

namespace dexih.remote.operations
{
    public class LiveDataItem
    {
        public long HubKey { get; set; }
        public long ApiKey { get; set; }
        public Transform Transform { get; set; }
    }
}