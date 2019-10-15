using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;

namespace dexih.remote.operations
{
    
    /// <summary>
    /// Background task
    /// First prepares (zips) files in the background
    /// Then sends a new task to send the data.
    /// </summary>
    public class DownloadFilesTask: ManagedObject
    {
        public DownloadFilesTask(ISharedSettings sharedSettings, string messageId, long hubKey, ConnectionFlatFile connectionFlatFile,
            FlatFile flatFile, EFlatFilePath path, string[] files, string responseUrl, string connectionId, string reference)
        {
            _sharedSettings = sharedSettings;
            _messageId = messageId;
            _hubKey = hubKey;
            _connectionFlatFile = connectionFlatFile;
            _flatFile = flatFile;
            _path = path;
            _files = files;
            _responseUrl = responseUrl;
            _connectionId = connectionId;
            _reference = reference;
        }
        
        
        private readonly ISharedSettings _sharedSettings;
        private readonly long _hubKey;
        private readonly string _messageId;
        private readonly ConnectionFlatFile _connectionFlatFile;
        private readonly FlatFile _flatFile;
        private readonly EFlatFilePath _path;
        private readonly string[] _files;
        private readonly string _responseUrl;
        private readonly string _connectionId;
        private readonly string _reference;
        
        
        public override async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            progress.Report(50, 1, "Preparing files...");

            var downloadStream = await _connectionFlatFile.DownloadFiles(_flatFile, _path, _files, _files.Length > 1);
            var filename = _files.Length == 1 ? _files[0] : _flatFile.Name + "_files.zip";

            progress.Report(100, 2, "Files ready for download...");

            await _sharedSettings.StartDataStream(_messageId, downloadStream, _responseUrl, "file", filename, cancellationToken);

            var url = $"{_sharedSettings.RemoteSettings.Network.ProxyUrl}/download/{_messageId}";
            
            var downloadMessage = new DownloadReadyMessage()
            {
                InstanceId = _sharedSettings.InstanceId,
                SecurityToken = _sharedSettings.SecurityToken,
                ConnectionId = _connectionId,
                Reference = _reference,
                HubKey = _hubKey,
                Url = url
            };
                    
            var returnValue = await _sharedSettings.PostAsync<DownloadReadyMessage, ReturnValue>("Remote/DownloadReady", downloadMessage, cancellationToken);
            if (!returnValue.Success)
            {
                throw new RemoteOperationException($"The file download did not completed.  {returnValue.Message}", returnValue.Exception);
            }
        }

        public override object Data { get; set; }
    }
}