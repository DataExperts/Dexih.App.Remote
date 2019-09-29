using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dexih.Utils.ManagedTasks;

namespace dexih.remote.operations
{
    
    /// <summary>
    /// Posts a stream to a url in the background.
    /// This is used to send data to the reverse proxy as a background process.
    /// </summary>
    public class PostDataTask: ManagedObject
    {
        public PostDataTask(HttpClient httpClient, Stream stream, string uploadUrl)
        {
            _httpClient = httpClient;
            _stream = stream;
            _uploadUrl = uploadUrl;
        }

        private readonly HttpClient _httpClient;
        private readonly Stream _stream;
        private readonly string _uploadUrl;
        
        public override async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            await _httpClient.PostAsync(_uploadUrl, new StreamContent(_stream), cancellationToken);
        }

        public override object Data { get; set; }
    }
}