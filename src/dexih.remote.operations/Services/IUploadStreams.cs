using System;
using System.IO;
using System.Threading.Tasks;

namespace dexih.remote.Operations.Services
{
    public interface IUploadStreams
    {
        string OriginUrl { get; set; }
        long MaxUploadSize { get; set; }
        UploadKeys SetUploadAction(Func<Stream, Task> processAction);
        Task ProcessUploadAction(string key, string securityKey, Stream stream);
    }
}