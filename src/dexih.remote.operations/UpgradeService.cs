using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using dexih.remote.Operations.Services;
using dexih.repository;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    public class UpgradeService: IHostedService, IDisposable
    {
        const string remoteBinary = "dexih.remote.zip";
        private const string updateDirectory = "upgrade";
        
            
        private Timer _timer;
        private ILogger<UpgradeService> _logger;
        private ISharedSettings _sharedSettings;
        private RemoteSettings _remoteSettings;
        private IApplicationLifetime _applicationLifetime;

        public UpgradeService(ISharedSettings sharedSettings, ILogger<UpgradeService> logger, IApplicationLifetime applicationLifetime)
        {
            _sharedSettings = sharedSettings;
            _remoteSettings = sharedSettings.RemoteSettings;
            _logger = logger;
            _applicationLifetime = applicationLifetime;
        }
        
        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (!_remoteSettings.AppSettings.AutoUpgrade)
            {
                return Task.CompletedTask;
            }

            if (cancellationToken.IsCancellationRequested) { return Task.CompletedTask; }

            _timer = new Timer(CheckUpgrade, null, TimeSpan.Zero, 
                TimeSpan.FromMinutes(5));
            
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _timer?.Change(Timeout.Infinite, 0);
            _logger.LogInformation("The upgrade service has stopped.");
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }

        public bool UpgradeRequired { get; set; } = false;

        private async void CheckUpgrade(object state)
        {
            // if an upgrade is required return the ExitCode.Upgrade value, which will be picked up by executing script to complete upgrade.
            try
            {
                var update = await _remoteSettings.CheckUpgrade();

                if (update)
                {
                    File.WriteAllText("latest_version.txt",
                        _remoteSettings.Runtime.LatestVersion + "\n" + _remoteSettings.Runtime.LatestDownloadUrl);
                    _logger?.LogWarning($"*****************   UPGRADE AVAILABLE ****************");
                    _logger?.LogWarning(
                        $"The local version of the remote agent is v{_remoteSettings.Runtime.Version}.");
                    _logger?.LogWarning(
                        $"The latest version of the remote agent is {_remoteSettings.Runtime.LatestVersion}.");
                    _logger?.LogWarning(
                        $"There is a newer release of the remote agent available at {_remoteSettings.Runtime.LatestDownloadUrl}.");

                    _timer?.Change(Timeout.Infinite, 0);
                    _logger?.LogWarning(
                        "The application will exit so an upgrade can be completed.  To skip upgrade checks include \"--upgrade=false\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");

                    //TODO Complete automatating of the upgrade
//                        _logger.LogInformation("Downloading latest binaries...");
//
//                        if (File.Exists(remoteBinary))
//                        {
//                            File.Delete(remoteBinary);
//                        }
//
//                        using (var client = new WebClient())
//                        {
//                            try
//                            {
//                                client.DownloadFile(_remoteSettings.Runtime.LatestDownloadUrl, remoteBinary);
//                            }
//                            catch (Exception ex)
//                            {
//                                _logger.LogError(ex, $"Could not download the remote agent binary at {_remoteSettings.Runtime.LatestDownloadUrl}");
//                                throw;
//                            }
//                        }
//
//                        _logger.LogInformation("Extracting latest binaries...");
//
//                        if (Directory.Exists(updateDirectory))
//                        {
//                            Directory.Delete(updateDirectory, true);
//                        }
//
//                        ZipFile.ExtractToDirectory(remoteBinary, updateDirectory);
//                        File.Delete(remoteBinary);

                    _sharedSettings.CompleteUpgrade = true;

                    // if auto upgrade is on, then shutdown the application so an upgrade can be completed.
                    _applicationLifetime.StopApplication();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"There was an error checking for update.  Message: {ex.Message}");
            }
        }


    }
}