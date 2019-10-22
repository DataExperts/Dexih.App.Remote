using System;
using System.IO;
using System.Threading.Tasks;

namespace dexih.remote.operations
{
    public class DownloadStream : IDisposable
    {
        public DownloadStream(string fileName, string type, Stream stream, bool isError)
        {
            AddedDateTime = DateTime.Now;
            FileName = fileName;
            Type = type;
            Stream = new BufferedStream(stream);
            IsError = isError;
        }
        public Stream Stream { get; set; }
        public string FileName { get; private set; }
        public string Type { get; set; }
        public DateTime AddedDateTime { get; private set; }
        
        public bool IsError { get; set; }

        /// <summary>
        /// Copy the uploaded stream for download.
        /// </summary>
        /// <param name="stream">stream to copy to</param>
        /// <param name="timeout">seconds to wait</param>
        /// <returns></returns>
        /// <exception cref="TimeoutException"></exception>
        public async Task CopyDownLoadStream(Stream stream, int timeout)
        {
            var count = 0;
            var maxCount = timeout * 10;
            while (Stream == null)
            {
                await Task.Delay(100);
                if (++count > maxCount)
                {
                    throw new TimeoutException("Timeout occurred waiting for download stream");
                }
            }
            await Stream.CopyToAsync(stream);
        }
        
        public void Dispose()
        {
            Stream?.Dispose();
        }
    }
}