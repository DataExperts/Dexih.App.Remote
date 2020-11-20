using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.repository;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    public class AutoStartService : IHostedService
    {
        private readonly ILiveApis _liveApis;
        private readonly RemoteSettings _remoteSettings;
        private readonly ILogger<AutoStartService> _logger;
        private readonly IRemoteOperations _remoteOperations;


        public AutoStartService(ISharedSettings sharedSettings, ILogger<AutoStartService> logger, ILiveApis liveApis, IRemoteOperations remoteOperations)
        {
            _remoteSettings = sharedSettings.RemoteSettings;
            _logger = logger;
            _liveApis = liveApis;
            _remoteOperations = remoteOperations;
        }
        
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The auto-start is starting.");

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.CompletedTask; 
            }

            var path = _remoteSettings.AutoStartPath();
            
            // load the api's
            var files = Directory.GetFiles(path, "dexih_api*.json");
            foreach (var file in files)
            {
                try
                {
                    var fileData = File.ReadAllText(file);
                    var autoStart = fileData.Deserialize<AutoStart>();
                    autoStart.Decrypt(_remoteSettings.AppSettings.EncryptionKey);
                    _logger.LogInformation($"Auto-starting the apn in file {file}");
                    _liveApis.ActivateApi(autoStart);
                }
                catch (Exception ex)
                {
                    _logger.LogError(500, ex, "Error auto-starting the file {0}: {1}", file, ex.Message);
                }
            }
            
            // load the datajobs's
            files = Directory.GetFiles(path, "dexih_datajob*.json");
            foreach (var file in files)
            {
                try
                {
                    var fileData = File.ReadAllText(file);
                    var autoStart = fileData.Deserialize<AutoStart>();
                    autoStart.Decrypt(_remoteSettings.AppSettings.EncryptionKey);
                    _logger.LogInformation($"Auto-starting the datajob in file {file}");
                    _remoteOperations.ActivateDatajob(autoStart);
                }
                catch (Exception ex)
                {
                    _logger.LogError(500, ex, "Error auto-starting the file {0}: {1}", file, ex.Message);
                }
            }
            
            _logger.LogInformation("The auto-start is started.");

            return Task.CompletedTask; 
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The auto-start service has stopped.");

            return Task.CompletedTask;
        }
        
    }
}