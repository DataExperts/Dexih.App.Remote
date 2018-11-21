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

        StreamSecurityKeys SetUploadAction(string name, Func<Stream, Task> processAction);
        Task ProcessUploadAction(string key, string securityKey, Stream stream);

        StreamSecurityKeys SetDownloadStream(string fileName, Stream stream);
        DownloadObject GetDownloadStream(string key, string securityKey);
    }
}