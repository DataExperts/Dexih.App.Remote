using System.Collections.Generic;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Transforms;

namespace dexih.remote.operations
{
    public class RemoteLibraries
    {
        public List<FunctionReference> Functions { get; set; }
        public List<ConnectionReference> Connections { get; set; }
        public List<TransformReference> Transforms { get; set; }
    }
}