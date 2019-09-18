using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.remote.Operations.Services;
using dexih.transforms;
using Dexih.Utils.Crypto;
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
        public DownloadFilesTask(ISharedSettings sharedSettings, long hubKey, ConnectionFlatFile connectionFlatFile,
            FlatFile flatFile, EFlatFilePath path, string[] files, DownloadUrl downloadUrl, string connectionId, string refeerence)
        {
            _sharedSettings = sharedSettings;
            _hubKey = hubKey;
            _connectionFlatFile = connectionFlatFile;
            _flatFile = flatFile;
            _path = path;
            _files = files;
            _downloadUrl = downloadUrl;
            _connectionId = connectionId;
            _reference = refeerence;
        }
        
        
        private readonly ISharedSettings _sharedSettings;
        private readonly long _hubKey;
        private readonly ConnectionFlatFile _connectionFlatFile;
        private readonly FlatFile _flatFile;
        private readonly EFlatFilePath _path;
        private readonly string[] _files;
        private readonly DownloadUrl _downloadUrl;
        private readonly string _connectionId;
        private readonly string _reference;
        
        
        public override async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            progress.Report(50, 1, "Preparing files...");

            var downloadStream = await _connectionFlatFile.DownloadFiles(_flatFile, _path, _files, _files.Length > 1);
            var filename = _files.Length == 1 ? _files[0] : _flatFile.Name + "_files.zip";

            progress.Report(100, 2, "Files ready for download...");

            var result = await _sharedSettings.StartDataStream(downloadStream, _downloadUrl, "file", filename, cancellationToken);

            var downloadMessage = new
            {
                _sharedSettings.InstanceId,
                _sharedSettings.SecurityToken,
                ConnectionId = _connectionId,
                Reference = _reference,
                _hubKey,
                Url = result
            };
                    
            var response = await _sharedSettings.PostAsync("Remote/DownloadReady", downloadMessage, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                throw new RemoteOperationException($"The file download did not complete as the http server returned the response {response.ReasonPhrase}.");
            }

            var returnValue = Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(), _sharedSettings.SessionEncryptionKey);
            if (!returnValue.Success)
            {
                throw new RemoteOperationException($"The file download did not completed.  {returnValue.Message}", returnValue.Exception);
            }
        }

        public override object Data { get; set; }
    }
}