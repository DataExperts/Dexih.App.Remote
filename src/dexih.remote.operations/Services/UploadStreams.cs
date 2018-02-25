using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.EntityFrameworkCore.Storage;

namespace dexih.remote.Operations.Services
{
    public class UploadStreams: IUploadStreams
    {
        private readonly ConcurrentDictionary<string, UploadObject> _uploadStreams;

        public UploadStreams()
        {
            _uploadStreams = new ConcurrentDictionary<string, UploadObject>();

            var cleanup = new Timer {Interval = 5000};
            cleanup.Elapsed += CleanUpOldStreams;
        }

        public string OriginUrl { get; set; }
        public long MaxUploadSize { get; set; }

        public UploadKeys SetUploadAction(Func<Stream, Task> processAction)
        {
            var uploadObject = new UploadObject(processAction);
            var key = Guid.NewGuid().ToString();
            _uploadStreams.TryAdd(key, uploadObject);

            return new UploadKeys(key, uploadObject.SecurityKey);
        }

        public async Task ProcessUploadAction(string key, string securityKey, Stream stream)
        {
            if (!_uploadStreams.TryRemove(key, out var uploadObject))
            {
                throw new Exception("The upload could not complete due to missing upload object.  This could be due to a timeout.");
                
            }
            
            if (securityKey == uploadObject.SecurityKey)
            {
                await uploadObject.ProcessAction.Invoke(stream);
            }
            else
            {
                throw new Exception("The upload could not complete due to mismatching security key.");
            }

        }

        private void CleanUpOldStreams(object o, EventArgs args)
        {
            foreach (var downloadObject in _uploadStreams)
            {
                if (downloadObject.Value.AddedDateTime.AddMinutes(5) < DateTime.Now)
                {
                    _uploadStreams.TryRemove(downloadObject.Key, out _);
                }
            }
        }
    }

    public class UploadObject
    {
        public UploadObject(Func<Stream, Task> processAction)
        {
            SecurityKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
            AddedDateTime = DateTime.Now;
            ProcessAction = processAction;
        }
        
        public string SecurityKey { get; private set; }
        public DateTime AddedDateTime { get; private set; }
        public Func<Stream, Task> ProcessAction { get; private set; }
    }

    public class UploadKeys
    {
        public UploadKeys(string key, string securityKey)
        {
            Key = key;
            SecurityKey = securityKey;
        }
        
        public string Key { get; set; }
        public string SecurityKey { get; set; }
    }
}