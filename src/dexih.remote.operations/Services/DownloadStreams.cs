using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Timers;
using Microsoft.EntityFrameworkCore.Storage;

namespace dexih.remote.Operations.Services
{
    public class DownloadStreams: IDownloadStreams
    {
        private readonly ConcurrentDictionary<string, DownloadObject> _downloadStreams;

        public DownloadStreams()
        {
            _downloadStreams = new ConcurrentDictionary<string, DownloadObject>();

            var cleanup = new Timer {Interval = 5000};
            cleanup.Elapsed += CleanUpOldStreams;
        }
        
        public (string key, string securityKey) SetDownloadStream(string fileName, Stream stream)
        {
            var downloadObject = new DownloadObject(fileName, stream);
            var key = Guid.NewGuid().ToString();
            _downloadStreams.TryAdd(key, downloadObject);

            return (key, downloadObject.SecurityKey);
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

        private void CleanUpOldStreams(object o, EventArgs args)
        {
            foreach (var downloadObject in _downloadStreams)
            {
                if (downloadObject.Value.AddedDateTime.AddMinutes(5) < DateTime.Now)
                {
                    _downloadStreams.TryRemove(downloadObject.Key, out _);
                }
            }
        }
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