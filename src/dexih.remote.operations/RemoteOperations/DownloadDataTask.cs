using System.Threading;
using System.Threading.Tasks;
using dexih.operations;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;

namespace dexih.remote.operations
{
    
    /// <summary>
    /// Background task
    /// First prepares (zips) files in the background
    /// Then sends a new task to send the data.
    /// </summary>
    public class DownloadDataTask: ManagedObject
    {
        public DownloadDataTask(ISharedSettings sharedSettings, string messageId, long hubKey, DownloadData downloadData, bool useProxy, string connectionId, string reference)
        {
            _messageId = messageId;
            _sharedSettings = sharedSettings;
            _hubKey = hubKey;
            _downloadData = downloadData;
            _useProxy = useProxy;
            _connectionId = connectionId;
            _reference = reference;
        }

        private string _messageId;
        private readonly ISharedSettings _sharedSettings;
        private readonly long _hubKey;
        private readonly DownloadData _downloadData;
        private readonly bool _useProxy;
        private readonly string _connectionId;
        private readonly string _reference;
        
        
        public override async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            progress.Report(50, 1, "Running data extract...");
            var downloadStream = await _downloadData.GetStream(cancellationToken);
            var filename = downloadStream.FileName;
            var stream = downloadStream.Stream;

            progress.Report(100, 2, "Download ready...");

            await _sharedSettings.StartDataStream(_messageId, stream, _useProxy, "file", filename, cancellationToken);
            var url = $"{_sharedSettings.RemoteSettings.Network.ProxyUrl}/download/{_messageId}";
                    
            var downloadMessage = new DownloadReadyMessage()
            {
                InstanceId =_sharedSettings.InstanceId,
                SecurityToken = _sharedSettings.SecurityToken,
                ConnectionId = _connectionId,
                Reference = _reference,
                HubKey = _hubKey,
                Url = url
            };

            var returnValue = await _sharedSettings.PostAsync<DownloadReadyMessage, ReturnValue>("Remote/DownloadReady", downloadMessage, cancellationToken);

            if (!returnValue.Success)
            {
                throw new RemoteOperationException($"The data download did not completed.  {returnValue.Message}", returnValue.Exception);
            }
        }

        public override object Data { get; set; }
    }


}