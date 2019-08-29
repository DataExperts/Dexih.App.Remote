using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using System.Timers;
using dexih.operations;
using dexih.repository;
using Microsoft.Extensions.Caching.Memory;

namespace dexih.remote.Operations.Services
{
    public class Streams: IStreams
    {
        internal static readonly TimeSpan timeout = TimeSpan.FromSeconds(5);
        
        private readonly IMemoryCache _memoryCache;

        public string OriginUrl { get; set; }
        public RemoteSettings RemoteSettings { get; set; }

        public Streams(IMemoryCache memoryCache)
        {
            _memoryCache = memoryCache;
        }
        
        public StreamSecurityKeys SetUploadAction(string name, Func<Stream, Task> processAction)
        {
            var uploadObject = new UploadObject(processAction);
            var key = Guid.NewGuid().ToString();

            _memoryCache.Set(key, uploadObject, timeout);

            return new StreamSecurityKeys(key, uploadObject.SecurityKey);
        }

        public async Task ProcessUploadAction(string key, string securityKey, Stream stream)
        {
            var uploadObject = _memoryCache.Get<UploadObject>(key);
            if (uploadObject == null)
            {
                throw new Exception("The upload could not complete due to missing upload object.  This could be due to a timeout.");
            }

            _memoryCache.Remove(key);

            if (securityKey == uploadObject.SecurityKey)
            {
                await uploadObject.ProcessAction.Invoke(stream);
            }
            else
            {
                throw new Exception("The upload could not complete due to mismatching security key.");
            }
        }

        public StreamSecurityKeys SetDownloadStream(string fileName, Stream stream)
        {
            var downloadObject = new DownloadObject(fileName, stream);
            var key = Guid.NewGuid().ToString();
            _memoryCache.Set(key, downloadObject, timeout);

            return new StreamSecurityKeys(key, downloadObject.SecurityKey);
        }

        public DownloadObject GetDownloadStream(string key, string securityKey)
        {
            var stream = _memoryCache.Get<DownloadObject>(key);
            if (stream == null)
            {
                throw new Exception("The download could not complete due to missing download stream.  This could be due to a timeout.");
            }
            
            
            if (securityKey == stream.SecurityKey)
            {
                return stream;
            }

            throw new Exception("The download could not complete due to mismatching security key.");
        }


    }

    public class UploadObject
    {
        public UploadObject(Func<Stream, Task> processAction)
        {
            SecurityKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey(); // the / values cause problems when 
            AddedDateTime = DateTime.Now;
            ProcessAction = processAction;
        }
        
        public string SecurityKey { get; private set; }
        public DateTime AddedDateTime { get; private set; }
        public Func<Stream, Task> ProcessAction { get; private set; }
    }

    
    public class DownloadObject : IDisposable
    {
        public DownloadObject(string fileName, Stream stream)
        {
            SecurityKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
            AddedDateTime = DateTime.Now;
            DownloadStream = stream;
            FileName = fileName;
        }
        
        public string SecurityKey { get; private set; }
        public Stream DownloadStream { get; private set; }
        public string FileName { get; private set; }
        public DateTime AddedDateTime { get; private set; }

        public void Dispose()
        {
            DownloadStream?.Dispose();
        }
    }
}