using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.operations;
using dexih.remote.Operations.Services;
using dexih.repository;
using dexih.transforms;
using dexih.transforms.Transforms;
using Dexih.Utils.Crypto;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    public class AutoStartService : IHostedService
    {
        private readonly ILiveApis _liveApis;
        private readonly RemoteSettings _remoteSettings;
        private readonly ISharedSettings _sharedSettings;
        private readonly ILogger<AutoStartService> _logger;
        private readonly IRemoteOperations _remoteOperations;


        public AutoStartService(ISharedSettings sharedSettings, ILogger<AutoStartService> logger, ILiveApis liveApis, IRemoteOperations remoteOperations)
        {
            _sharedSettings = sharedSettings;
            
            _remoteSettings = _sharedSettings.RemoteSettings;
            _logger = logger;
            _liveApis = liveApis;
            _remoteOperations = remoteOperations;
        }
        
        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested) { return Task.CompletedTask; }

            var path = _remoteSettings.AutoStartPath();
            
            // load the api's
            var files = Directory.GetFiles(path, "dexih_api*.json");
            foreach (var file in files)
            {
                try
                {
                    var fileData = File.ReadAllText(file);
                    var autoStart = Json.JTokenToObject<AutoStart>(fileData, _remoteSettings.AppSettings.EncryptionKey);
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
                    var autoStart = Json.JTokenToObject<AutoStart>(fileData, _remoteSettings.AppSettings.EncryptionKey);
                    _logger.LogInformation($"Auto-starting the datajob in file {file}");
                    _remoteOperations.ActivateDatajob(autoStart);
                }
                catch (Exception ex)
                {
                    _logger.LogError(500, ex, "Error auto-starting the file {0}: {1}", file, ex.Message);
                }
            }
            
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("AutoStart service has stopped.");

            return Task.CompletedTask;
        }
        
    }
}