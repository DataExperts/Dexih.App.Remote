using System;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.repository;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Open.Nat;

namespace dexih.remote.operations
{
    public class HttpService : BackgroundService
    {
        private readonly ISharedSettings _sharedSettings;
        private readonly ILiveApis _liveApis;
        private readonly IMemoryCache _memoryCache;
        
        private readonly RemoteSettings _remoteSettings;
        private readonly ILogger<HttpService> _logger;
        
        public HttpService(ISharedSettings sharedSettings, ILogger<HttpService> logger, ILiveApis liveApis, IMemoryCache memoryCache)
        {
            _sharedSettings = sharedSettings;
            _liveApis = liveApis;
            _remoteSettings = _sharedSettings.RemoteSettings;
            _logger = logger;
            _memoryCache = memoryCache;
        }
        
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                
                var connectionResult = await _sharedSettings.WaitForLogin(false, cancellationToken);

                if (connectionResult != EConnectionResult.Connected)
                {
                    return;
                }

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
                        using (var cert = new X509Certificate2(certificatePath, _remoteSettings.Network.CertificatePassword))
                        {
                            // var effectiveDate = DateTime.Parse(cert.GetEffectiveDateString());
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
                                    s.AddSingleton(_sharedSettings);
                                    s.AddSingleton(_liveApis);
                                    s.AddSingleton(_memoryCache);
                                    s.AddSingleton(_logger);
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
                    }
                }

                _logger.LogInformation("HttpService has stopped.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    $"The following error was encountered with the http listening service.  Http connections will not be available.  {ex.Message}");            }
        }
        
        private async Task RenewSslCertificate(string certificatePath, CancellationToken cancellationToken)
        {
            try
            {
                var details = new RenewSslCertificateModel()
                {
                    Domain = _remoteSettings.Network.DynamicDomain,
                    Password = _remoteSettings.Network.CertificatePassword
                };

                _logger.LogInformation("Attempting to generate a new ssl certificate...");

                var response =
                    await _sharedSettings.PostAsync("Remote/GenerateCertificate", details, cancellationToken);

                if (response != null)
                {
                    if (response.Content.Headers.ContentType.MediaType == "application/json")
                    {
                        var jsonContent = await response.Content.ReadAsStringAsync();
                        var message = JsonExtensions.Deserialize<ReturnValue>(jsonContent);
                        _logger.LogError("Ssl request generated the following error: " + message.ExceptionDetails);
                        throw new RemoteSecurityException(message.Message, message.Exception);
                    }

                    var certificate = await response.Content.ReadAsByteArrayAsync();

                    try
                    {
                        using (var cert =
                            new X509Certificate2(certificate, _remoteSettings.Network.CertificatePassword))
                        {
                            var effectiveDate = DateTime.Parse(cert.GetEffectiveDateString());
                            var expiresDate = DateTime.Parse(cert.GetExpirationDateString());

                            // if cert expires in next 14 days, then renew.
                            if (DateTime.Now > effectiveDate && DateTime.Now < expiresDate)
                            {
                                await File.WriteAllBytesAsync(certificatePath,
                                    certificate, cancellationToken);

                                _logger.LogInformation(
                                    $"A new Ssl certificate has been successfully created at {certificatePath}.");
                            }
                            else
                            {
                                var reason =
                                    $"A new Ssl certificate was download but has effective dates between {effectiveDate} and {expiresDate}.  This certificate has not been saved.";
                                _logger.LogCritical(reason);
                                throw new RemoteSecurityException(reason);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        var reason =
                            $"There was an error with the downloaded SSL certificate.  {ex.Message}";
                        _logger.LogCritical(reason, ex);
                        throw new RemoteSecurityException(reason, ex);
                    }
                }
                else
                {
                    var reason = $"The ssl certificate renewal failed.  {response.ReasonPhrase}.";
                    _logger.LogCritical(reason);
                    throw new RemoteSecurityException(reason);
                }
            }
            catch (Exception ex)
            {
                _logger.LogCritical("There was an issue generating a new SSL certificate.  The existing certificate will be used, or if there is not a current certificate, then HTTP connections will not be available.", ex);
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