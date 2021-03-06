using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using dexih.repository;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    
    /// <summary>
    /// If a newer version of the binaries exist.  This copies them to a directory "update", and then initiates an exit.
    /// </summary>
    public class UpgradeService: IHostedService, IDisposable
    {
        const string RemoteBinary = "dexih.remote.zip";
        private const string UpdateDirectory = "update";
        private string _updateVersion;
            
        private Timer _timer;
        private readonly ILogger<UpgradeService> _logger;
        private readonly RemoteSettings _remoteSettings;
        private readonly IHostApplicationLifetime _applicationLifetime;
        private readonly ProgramExit _programExit;
        private readonly IHttpClientFactory _clientFactory;

        public UpgradeService(ISharedSettings sharedSettings, ILogger<UpgradeService> logger, IHostApplicationLifetime applicationLifetime, ProgramExit programExit, IHttpClientFactory clientFactory)
        {
            _remoteSettings = sharedSettings.RemoteSettings;
            _logger = logger;
            _applicationLifetime = applicationLifetime;
            _programExit = programExit;
            _clientFactory = clientFactory;
        }
        
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The upgrade service is starting.");

            if (cancellationToken.IsCancellationRequested) { return Task.CompletedTask; }

            _timer = new Timer(CheckUpgrade, null, TimeSpan.Zero, 
                TimeSpan.FromMinutes(5));
            
            _logger.LogInformation("The upgrade service is started.");

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The upgrade service is stopping.");

            _timer?.Change(Timeout.Infinite, 0);

            // check one last time before exiting.
            CheckUpgrade(null);
            
            _logger.LogInformation("The upgrade service has stopped.");
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }

        public bool UpgradeRequired { get; set; } = false;

        public async void CheckUpgrade(object state)
        {
            // if an upgrade is required return the ExitCode.Upgrade value, which will be picked up by executing script to complete upgrade.
            try
            {
                _logger?.LogTrace("Check upgrade started.");

                var update = await _remoteSettings.CheckUpgrade(_logger, _clientFactory);

                if (update && _updateVersion != _remoteSettings.Runtime.LatestVersion)
                {
                    _updateVersion = _remoteSettings.Runtime.LatestVersion;
                    
                    File.WriteAllText("latest_version.txt",
                        _remoteSettings.Runtime.LatestVersion + "\n" + _remoteSettings.Runtime.LatestDownloadUrl);
                    _logger?.LogWarning($"*****************   UPGRADE AVAILABLE ****************");
                    _logger?.LogWarning(
                        $"The local version of the remote agent is v{_remoteSettings.Runtime.Version}.");
                    _logger?.LogWarning(
                        $"The latest version of the remote agent is {_remoteSettings.Runtime.LatestVersion}.");
//                    _logger?.LogWarning(
//                        $"There is a newer release of the remote agent available at {_remoteSettings.Runtime.LatestDownloadUrl}.");
//
//                    _timer?.Change(Timeout.Infinite, 0);
//                    _logger?.LogWarning(
//                        "The application will exit so an upgrade can be completed.  To skip upgrade checks include \"--upgrade=false\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");

                    if (_remoteSettings.AppSettings.AutoUpgrade)
                    {
                        _logger.LogInformation("Downloading latest binaries...");

                        if (File.Exists(RemoteBinary))
                        {
                            File.Delete(RemoteBinary);
                        }

                        using (var client = new WebClient())
                        {
                            try
                            {
                                client.DownloadFile(_remoteSettings.Runtime.LatestDownloadUrl, RemoteBinary);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex,
                                    $"Could not download the remote agent binary at {_remoteSettings.Runtime.LatestDownloadUrl}");
                                throw;
                            }
                        }

                        _logger.LogInformation("Extracting latest binaries...");

                        if (Directory.Exists(UpdateDirectory))
                        {
                            Directory.Delete(UpdateDirectory, true);
                        }

                        ZipFile.ExtractToDirectory(RemoteBinary, UpdateDirectory);

                        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        {
                            var remoteAgentPath = Path.Combine(UpdateDirectory, "dexih.remote");
                            var process = new Process
                            {
                                StartInfo = new ProcessStartInfo
                                {
                                    RedirectStandardOutput = true,
                                    UseShellExecute = false,
                                    CreateNoWindow = true,
                                    WindowStyle = ProcessWindowStyle.Hidden,
                                    FileName = "chmod",
                                    Arguments = $"+x {remoteAgentPath}"
                                }
                            };

                            process.Start();
                            process.WaitForExit();
                        }

                        File.Delete(RemoteBinary);

                        _programExit.CompleteUpgrade = true;

                        _logger?.LogWarning(
                            $"The new version has been downloaded to {Path.GetFullPath(UpdateDirectory)}.");
                        _timer?.Change(Timeout.Infinite, 0);
                        _logger?.LogWarning(
                            "The application will exit so an upgrade can be completed.  To skip upgrade checks include \"--upgrade=false\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");

                        // if auto upgrade is on, then shutdown the application so an upgrade can be completed.
                        _applicationLifetime.StopApplication();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"There was an error checking for update.  Message: {ex.Message}");
            }

            _logger?.LogTrace("Check upgrade finished.");

        }


    }
}