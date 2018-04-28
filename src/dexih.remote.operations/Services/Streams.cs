using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Timers;
using dexih.repository;
using Microsoft.EntityFrameworkCore.Storage;

namespace dexih.remote.Operations.Services
{
    public class Streams: IStreams
    {
        private readonly ConcurrentDictionary<string, UploadObject> _uploadStreams;
        private readonly ConcurrentDictionary<string, DownloadObject> _downloadStreams;

        public string OriginUrl { get; set; }
        public RemoteSettings RemoteSettings { get; set; }

        public Streams()
        {
            _uploadStreams = new ConcurrentDictionary<string, UploadObject>();
            _downloadStreams = new ConcurrentDictionary<string, DownloadObject>();

            var cleanup = new Timer {Interval = 5000};
            cleanup.Elapsed += CleanUpOldStreams;
        }


        public StreamSecurityKeys SetUploadAction(string name, Func<Stream, Task> processAction)
        {
            var uploadObject = new UploadObject(processAction);
            var key = Guid.NewGuid().ToString();
            _uploadStreams.TryAdd(key, uploadObject);

            return new StreamSecurityKeys(key, uploadObject.SecurityKey);
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
            
            foreach (var downloadObject in _downloadStreams)
            {
                if (downloadObject.Value.AddedDateTime.AddMinutes(5) < DateTime.Now)
                {
                    _downloadStreams.TryRemove(downloadObject.Key, out _);
                }
            }
        }

        public StreamSecurityKeys SetDownloadStream(string fileName, Stream stream)
        {
            var downloadObject = new DownloadObject(fileName, stream);
            var key = Guid.NewGuid().ToString();
            _downloadStreams.TryAdd(key, downloadObject);

            return new StreamSecurityKeys(key, downloadObject.SecurityKey);
        }

        public (string fileName, Stream stream) GetDownloadStream(string key, string securityKey)
        {
            if (!_downloadStreams.TryRemove(key, out var stream))
            {
                throw new Exception("The download could not complete due to missing download stream.  This could be due to a timeout.");
                
            }
            
            if (securityKey == stream.SecurityKey)
            {
                return (stream.FileName, stream.DownloadStream);
            }

            throw new Exception("The download could not complete due to mismatching security key.");
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

    
    public class DownloadObject
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
    }
}