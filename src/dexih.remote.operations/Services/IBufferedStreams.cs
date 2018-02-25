using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace dexih.remote.Operations.Services
{
    public interface IBufferedStreams
    {
        (string key, string securityKey) SetDownloadBuffer(BufferBlock<object> bufferBlock);
        Task<(bool moreData, object buffer)> GetDownloadBuffer(string key, string securityKey, CancellationToken cancellationToken);
    }
}