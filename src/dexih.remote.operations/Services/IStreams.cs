using System;
using System.IO;
using System.Threading.Tasks;
using dexih.repository;

namespace dexih.remote.Operations.Services
{
    public interface IStreams
    {
        string OriginUrl { get; set; }
        RemoteSettings RemoteSettings { get; set; }

        StreamSecurityKeys SetUploadAction(Func<Stream, Task> processAction);
        Task ProcessUploadAction(string key, string securityKey, Stream stream);

        StreamSecurityKeys SetDownloadStream(string fileName, Stream stream);
        (string fileName, Stream stream) GetDownloadStream(string key, string securityKey);
    }
}