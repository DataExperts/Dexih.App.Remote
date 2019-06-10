using System;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using dexih.remote.Operations.Services;
using dexih.repository;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Open.Nat;

namespace dexih.remote.operations.Services
{
    public class HttpService : BackgroundService
    {
        private ISharedSettings _sharedSettings;
        private IStreams _streams;
        private ILiveApis _liveApis;
        
        private RemoteSettings _remoteSettings;
        private ILogger<HttpService> _logger;
        
        public HttpService(ISharedSettings sharedSettings, ILogger<HttpService> logger, IStreams streams, ILiveApis liveApis)
        {
            _sharedSettings = sharedSettings;
            _streams = streams;
            _liveApis = liveApis;
            _remoteSettings = _sharedSettings.RemoteSettings;
            _logger = logger;
        }
        
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested) { return; }

            var useHttps = !string.IsNullOrEmpty(_remoteSettings.Network.CertificateFilename);
            var certificatePath = _remoteSettings.CertificateFilePath();

            _logger.LogInformation($"Using the ssl certificate at {certificatePath}");

            if (_remoteSettings.Network.AutoGenerateCertificate)
            {
                // if no certificate name or password are specified, generate them automatically.
                if (string.IsNullOrEmpty(_remoteSettings.Network.CertificateFilename))
                {
                    throw new RemoteSecurityException(
                        "The applicationSettings -> Network -> AutoGenerateCertificate is true, however there no setting for CertificateFilename");
                }

                if (string.IsNullOrEmpty(_remoteSettings.Network.CertificatePassword))
                {
                    throw new RemoteSecurityException(
                        "The applicationSettings -> Network -> AutoGenerateCertificate is true, however there no setting for CertificateFilename");
                }

                useHttps = true;

                var renew = false;

                if (File.Exists(certificatePath))
                {
                    // Create a collection object and populate it using the PFX file
                    using (var cert = new X509Certificate2(
                        certificatePath,
                        _remoteSettings.Network.CertificatePassword))
                    {
                        var effectiveDate = DateTime.Parse(cert.GetEffectiveDateString());
                        var expiresDate = DateTime.Parse(cert.GetExpirationDateString());

                        // if cert expires in next 14 days, then renew.
                        if (DateTime.Now > expiresDate.AddDays(-14))
                        {
                            renew = true;
                        }
                    }
                }
                else
                {
                    renew = true;
                }

                // generate a new ssl certificate
                if (renew)
                {
                    await RenewSslCertificate(certificatePath, cancellationToken);
                }
            }

            if (_remoteSettings.Network.EnforceHttps)
            {
                if (string.IsNullOrEmpty(_remoteSettings.Network.CertificateFilename))
                {
                    throw new RemoteSecurityException(
                        "The server requires https, however a CertificateFile name is not specified.");
                }

                if (!File.Exists(certificatePath))
                {
                    throw new RemoteSecurityException(
                        $"The certificate with the filename {certificatePath} does not exist.");
                }

                using (var cert = new X509Certificate2(
                    certificatePath,
                    _remoteSettings.Network.CertificatePassword))
                {
                    var effectiveDate = DateTime.Parse(cert.GetEffectiveDateString());
                    var expiresDate = DateTime.Parse(cert.GetExpirationDateString());

                    if (DateTime.Now < effectiveDate)
                    {
                        throw new RemoteSecurityException(
                            $"The certificate with the filename {certificatePath} is not valid until {effectiveDate}.");
                    }


                    if (DateTime.Now > expiresDate)
                    {
                        throw new RemoteSecurityException(
                            $"The certificate with the filename {certificatePath} expired on {expiresDate}.");
                    }
                }
            }

            if (!useHttps && _remoteSettings.Network.EnforceHttps)
            {
                throw new RemoteSecurityException(
                    "The remote agent is set to EnforceHttps, however no SSL certificate was found or able to be generated.");
            }

            if (!useHttps || File.Exists(certificatePath))
            {
                if (_remoteSettings.Network.DownloadPort == null)
                {
                    throw new RemoteSecurityException("The web server download port was not set.");
                }

                var port = _remoteSettings.Network.DownloadPort.Value;

                if (_remoteSettings.Network.EnableUPnP)
                {
                    await EnableUPnp(port);
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        _logger.LogInformation("Starting the data upload/download web server.");
                        var host = new WebHostBuilder()
                            .UseStartup<Startup>()
                            .ConfigureServices(s =>
                            {
                                s.AddSingleton(_streams);
                                s.AddSingleton(_liveApis);
                            })
                            .UseKestrel(options =>
                            {
                                options.Listen(IPAddress.Any,
                                    _remoteSettings.Network.DownloadPort ?? 33944,
                                    listenOptions =>
                                    {
                                        // if there is a certificate then default to https
                                        if (useHttps)
                                        {
                                            listenOptions.UseHttps(
                                                certificatePath,
                                                _remoteSettings.Network.CertificatePassword);
                                        }
                                    });
                            })
                            .UseUrls((useHttps ? "https://*:" : "http://*:") + port)
                            .Build();

                        await host.RunAsync(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"The following error was encountered with the http listening service.  {ex.Message}");
                        throw;
                    }
                }
            }
            
            _logger.LogInformation("HttpService has stopped.");
        }
        
        private async Task RenewSslCertificate(string certificatePath, CancellationToken cancellationToken)
        {
            var details = new
            {
                Domain = _remoteSettings.Network.DynamicDomain,
                Password = _remoteSettings.Network.CertificatePassword
            };

            _logger.LogInformation("Attempting to generate a new ssl certificate...");

            var response = await _sharedSettings.PostAsync("Remote/GenerateCertificate", details, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                if (response.Content.Headers.ContentType.MediaType == "application/json")
                {
                    var jsonContent = await response.Content.ReadAsStringAsync();
                    var message = JsonConvert.DeserializeObject<ReturnValue>(jsonContent);
                    _logger.LogDebug("Generate certificate details: " + message.ExceptionDetails);
                    throw new RemoteSecurityException(message.Message, message.Exception);
                }

                var certificate = await response.Content.ReadAsByteArrayAsync();
                File.WriteAllBytes(certificatePath,
                    certificate);
                
                _logger.LogInformation($"A new Ssl certificate has been successfully created at {certificatePath}.");
            }
            else
            {
                throw new RemoteSecurityException(
                    $"The ssl certificate renewal failed.  {response.ReasonPhrase}.");
            }
        }
        
        /// <summary>
        /// Create a uPnp mapping to a NAT device
        /// </summary>
        /// <returns></returns>
        private async Task EnableUPnp(int port)
        {
            try
            {
                var discoverer = new NatDiscoverer();
                var cts = new CancellationTokenSource(10000);
                var device = await discoverer.DiscoverDeviceAsync(PortMapper.Upnp, cts);

                if (cts.IsCancellationRequested)
                {
                    _logger.LogError(10, $"A timeout occurred mapping uPnp port");
                    return;
                }

                var ip = await device.GetExternalIPAsync();
                var exists = await device.GetSpecificMappingAsync(Protocol.Tcp, port);

                if (exists == null)
                {
                    await device.CreatePortMapAsync(new Mapping(Protocol.Tcp, port, port,
                        "Data Experts Remote Agent"));

                    _logger.LogInformation(
                        $"A new uPnp mapping to {port} has been created, and is listening from the public ip {ip}");
                }
                else
                {
                    _logger.LogInformation(
                        $"The uPnp mapping already exists: {exists} to the public ip {ip}");
                }
            }
            catch (NatDeviceNotFoundException)
            {
                _logger.LogWarning("The EnableUPnP is set to true in the appsettings.json file, however there was no NAT (Network Address Translation) device found on the network.");
            }
            catch (Exception ex)
            {
                _logger.LogError(10, ex, $"Error mapping uPnP port: {ex.Message}");
            }
        }

    }
    
    
}