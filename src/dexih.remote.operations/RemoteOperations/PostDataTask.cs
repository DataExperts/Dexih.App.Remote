using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;

namespace dexih.remote.operations
{
    
    /// <summary>
    /// Posts a stream to a url in the background.
    /// This is used to send data to the reverse proxy as a background process.
    /// </summary>
    public class PostDataTask: ManagedObject
    {
        public PostDataTask(HttpClient httpClient, Stream stream, string uploadUrl, string errorUrl, bool isError)
        {
            _httpClient = httpClient;
            _stream = stream;
            _uploadUrl = uploadUrl;
            _errorUrl = errorUrl;
            _isError = isError;
        }

        private readonly HttpClient _httpClient;
        private readonly Stream _stream;
        private readonly string _uploadUrl;
        private readonly string _errorUrl;
        private readonly bool _isError;
        
        public override async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            try
            {
                await _httpClient.PostAsync(_isError ? _errorUrl : _uploadUrl, new StreamContent(_stream), cancellationToken);
            }
            catch (Exception ex)
            {
                var returnValue = new ReturnValue(false, "Error: " + ex.Message, ex);
                await _httpClient.PostAsync(_errorUrl, new StringContent(returnValue.Serialize()), cancellationToken);
            }
        }

        public override object Data { get; set; }
    }
}