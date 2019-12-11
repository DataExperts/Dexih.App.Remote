using System;
using System.IO;
using Microsoft.Extensions.Caching.Memory;

namespace dexih.remote.operations
{
    /// <summary>
    /// Wraps a stream, and stores the result in the cache.
    /// </summary>
    public class CacheStream: Stream
    {
        private readonly Stream _inStream;
        private readonly IMemoryCache _memoryCache;
        private readonly string _cacheKey;
        private readonly int _cacheSeconds;
        private readonly MemoryStream _memoryStream;
        
        public CacheStream(Stream inStream, IMemoryCache memoryCache, string cacheKey, int cacheSeconds)
        {
            _inStream = inStream;
            _memoryCache = memoryCache;
            _cacheKey = cacheKey;
            _cacheSeconds = cacheSeconds;
            _memoryStream = new MemoryStream();
        }
        
        public override void Flush()
        {
            _inStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var returnValue = _inStream.Read(buffer, offset, count);

            if (returnValue == 0)
            {
                _memoryCache.Set(_cacheKey, _memoryStream.ToArray(), TimeSpan.FromSeconds(_cacheSeconds));
            }

            _memoryStream.Write(buffer, 0, returnValue);
            return returnValue;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _inStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _inStream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => _inStream.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _inStream.Length;

        public override long Position
        {
            get => _inStream.Position;
            set => _inStream.Position = value;
        }
    }
}