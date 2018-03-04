//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.IO;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Threading.Tasks.Dataflow;
//using Microsoft.CodeAnalysis.CSharp.Syntax;
//using Microsoft.EntityFrameworkCore.Storage;
//using Timer = System.Timers.Timer;
//
//namespace dexih.remote.Operations.Services
//{
//    public class BufferedStreams: IBufferedStreams
//    {
//        private readonly ConcurrentDictionary<string, BufferedObject> _bufferedStreams;
//
//        public BufferedStreams()
//        {
//            _bufferedStreams = new ConcurrentDictionary<string, BufferedObject>();
//
//            var cleanup = new Timer {Interval = 50000};
//            cleanup.Elapsed += CleanUpOldStreams;
//        }
//        
//        public (string key, string securityKey) SetDownloadBuffer(BufferBlock<object> bufferBlock)
//        {
//            var bufferedObject = new BufferedObject(bufferBlock);
//            var key = Guid.NewGuid().ToString();
//            _bufferedStreams.TryAdd(key, bufferedObject);
//
//            return (key, bufferedObject.SecurityKey);
//        }
//
//        public async Task<(bool moreData, object buffer)> GetDownloadBuffer(string key, string securityKey, CancellationToken cancellationToken)
//        {
//            var bufferFound = _bufferedStreams.TryGetValue(key, out var bufferObject);
//            
//            if (!bufferFound)
//            {
//                throw new Exception("The buffer could not found.");
//            }
//            
//            if (securityKey == bufferObject.SecurityKey)
//            {
//                var buffer = bufferObject.BufferBlock;
//                
//                var moreData = await buffer.OutputAvailableAsync(cancellationToken);
//
//                if(moreData) 
//                {
//                    var popResult = await buffer.ReceiveAsync(cancellationToken);
//
//                    if (buffer.Completion.IsFaulted)
//                    {
//                        throw buffer.Completion.Exception;
//                    }
//
//                    return (true, popResult);
//                }
//                else
//                {
//                    _bufferedStreams.TryRemove(key, out _);
//                    
//                    if (buffer.Completion.IsFaulted)
//                    {
//                        throw buffer.Completion.Exception;
//                    }
//                    
//                    return (false, null);
//                }
//            }
//
//            throw new Exception("The download could not complete due to mismatching security key.");
//        }
//
//        private void CleanUpOldStreams(object o, EventArgs args)
//        {
//            foreach (var downloadObject in _bufferedStreams)
//            {
//                if (downloadObject.Value.AddedDateTime.AddMinutes(5) < DateTime.Now)
//                {
//                    _bufferedStreams.TryRemove(downloadObject.Key, out _);
//                }
//            }
//        }
//    }
//
//    public class BufferedObject
//    {
//        public BufferedObject(BufferBlock<object> bufferBlock)
//        {
//            SecurityKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
//            AddedDateTime = DateTime.Now;
//            BufferBlock = bufferBlock;
//        }
//        
//        public string SecurityKey { get; private set; }
//        public BufferBlock<object> BufferBlock { get; private set; }
//        public DateTime AddedDateTime { get; private set; }
//    }
//}