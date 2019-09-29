using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dexih.Utils.ManagedTasks;

namespace dexih.remote.operations
{
    public class UploadDataTask: ManagedObject
    {

        public UploadDataTask(HttpClient httpClient, Func<Stream, Task> uploadAction, string uploadUrl)
        {
            _httpClient = httpClient;
            _uploadAction = uploadAction;
            _uploadUrl = uploadUrl;
        }

        private readonly HttpClient _httpClient;
        private readonly Func<Stream, Task> _uploadAction;
        private readonly string _uploadUrl;
        
        public override async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            var result = await _httpClient.GetAsync(_uploadUrl, cancellationToken);
            await _uploadAction.Invoke(await result.Content.ReadAsStreamAsync());
        }

        public override object Data { get; set; }
    }
}