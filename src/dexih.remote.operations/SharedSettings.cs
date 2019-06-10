using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Policy;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.remote.operations;
using dexih.repository;
using Dexih.Utils.Crypto;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using IApplicationLifetime = Microsoft.AspNetCore.Hosting.IApplicationLifetime;

namespace dexih.remote.Operations.Services
{
    public interface ISharedSettings
    {
        Task<HttpResponseMessage> PostAsync(string uri, object data, CancellationToken cancellationToken);
        Task<HttpResponseMessage> PostAsync(string uri, HttpContent content, CancellationToken cancellationToken);

        string SessionEncryptionKey { get; }
        
        string SecurityToken { get; set; }

        RemoteSettings RemoteSettings { get; }
        
        CookieContainer CookieContainer { get; }

        Task<EConnectionResult> WaitForLogin(bool reconnect = false, CancellationToken cancellationToken = default);
        
        string BaseUrl { get; }

        bool CompleteUpgrade { get; set; }
        void ResetConnection();
    }

    public class SharedSettings : ISharedSettings, IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<SharedSettings> _logger;
        private readonly IHost _host;
        private readonly string _apiUri;
        private readonly SemaphoreSlim _loginSemaphore;
        private EConnectionResult _connectionStatus = EConnectionResult.Disconnected;

        public string SessionEncryptionKey { get; }
        public string SecurityToken { get; set; }
        public RemoteSettings RemoteSettings { get; }

        public CookieContainer CookieContainer { get; }

        public string BaseUrl { get; }
        
        public bool CompleteUpgrade { get; set; }

        public SharedSettings(IConfiguration configuration, ILogger<SharedSettings> logger, IHost host)
        {
            _logger = logger;
            _host = host;

            SessionEncryptionKey = EncryptString.GenerateRandomKey();
            RemoteSettings = configuration.Get<RemoteSettings>();

            var url = RemoteSettings.AppSettings.WebServer;
            if (url.Substring(url.Length - 1) != "/") url += "/";
            BaseUrl = url;

            _apiUri = url + "api/";

            CookieContainer = new CookieContainer();
            var handler = new HttpClientHandler
            {
                CookieContainer = CookieContainer
            };

            //Login to the web server to receive an authenticated cookie.
            _httpClient = new HttpClient(handler);
            _loginSemaphore = new SemaphoreSlim(1, 1);
        }
        
        public void Dispose()
        {
            _httpClient.Dispose();
            _loginSemaphore.Dispose();
        }
        
        public Task<HttpResponseMessage> PostAsync(string uri, object data, CancellationToken cancellationToken)
        {
            var messagesString = Json.SerializeObject(data, SessionEncryptionKey);
            var jsonContent = new StringContent(messagesString, Encoding.UTF8, "application/json");

            return _httpClient.PostAsync(_apiUri + uri, jsonContent, cancellationToken);
        }

        public Task<HttpResponseMessage> PostAsync(string uri, HttpContent content, CancellationToken cancellationToken)
        {
            var messagesString = Json.SerializeObject(content, SessionEncryptionKey);
            var jsonContent = new StringContent(messagesString, Encoding.UTF8, "application/json");

            return _httpClient.PostAsync(_apiUri + uri, jsonContent, cancellationToken);
        }

        public void ResetConnection()
        {
            _connectionStatus = EConnectionResult.Disconnected;
        }
        
        public async Task<EConnectionResult> WaitForLogin(bool reconnect = false, CancellationToken cancellationToken = default)
        {
            // check if we are already connected
            if (!reconnect && _connectionStatus == EConnectionResult.Connected)
            {
                return EConnectionResult.Connected;
            }

            try
            {
                // the login semaphore only allows one login attempt simultaneously
                await _loginSemaphore.WaitAsync(cancellationToken);

                _connectionStatus = EConnectionResult.Connecting;

                var connectionResult = EConnectionResult.Disconnected;

                var retryStarted = false;

                while (connectionResult != EConnectionResult.Connected)
                {
                    connectionResult = await LoginAsync(retryStarted, cancellationToken);
                    switch (connectionResult)
                    {
                        case EConnectionResult.InvalidCredentials:
                            _logger.LogWarning("Invalid credentials... terminating service.");

                            var applicationLifetime = _host.Services.GetService<IApplicationLifetime>();
                            applicationLifetime.StopApplication();

                            _loginSemaphore.Release();
                            return EConnectionResult.InvalidCredentials;

                        case EConnectionResult.InvalidLocation:
                            if (!retryStarted)
                            {
                                _logger.LogWarning("Invalid location... web server might be down... retrying...");
                            }

                            break;
                        default:
                            if (!retryStarted)
                            {
                                _logger.LogWarning("Unhandled exception on remote server.. retrying...");
                            }

                            break;
                    }

                    await Task.Delay(5000, cancellationToken);
                    retryStarted = true;
                }
            }
            finally
            {
                _loginSemaphore.Release();    
            }

            return EConnectionResult.Connected;
        }

        /// <summary>
        /// Authenticates and logs the user in
        /// </summary>
        /// <param name="generateUserToken">Create a new user authentication token</param>
        /// <param name="silentLogin"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The connection result, and a new user token if generated.</returns>
        private async Task<EConnectionResult> LoginAsync(bool retryStarted, CancellationToken cancellationToken)
        {
            try
            {
                if (!retryStarted)
                {
                    _logger.LogInformation(1,
                        $"Connecting to {RemoteSettings.AppSettings.WebServer} with user {RemoteSettings.AppSettings.User}");
                    _logger.LogInformation($"This remote agent is named: \"{RemoteSettings.AppSettings.Name}\"");
                }

                var messagesString = Json.SerializeObject(RemoteSettings, SessionEncryptionKey);
                var content = new StringContent(messagesString, Encoding.UTF8, "application/json");

                HttpResponseMessage response;
                try
                {
                    response = await PostAsync("Remote/Login", RemoteSettings, cancellationToken);
                }
                catch (HttpRequestException ex)
                {
                    if (!retryStarted)
                        _logger.LogCritical(10, ex,
                            "Could not connect to the server at location: {server}, with the message: {message}",
                            "/Remote/Login", ex.Message);
                    return EConnectionResult.InvalidLocation;
                }
                catch (Exception ex)
                {
                    if (!retryStarted)
                        _logger.LogCritical(10, ex, "Internal error connecting to the server at location: {0}",
                            "/Remote/Login");
                    return EConnectionResult.InvalidLocation;
                }

                if (!response.IsSuccessStatusCode)
                {
                    if (!retryStarted)
                        _logger.LogCritical(4,
                            $"Could not connect with server.  Status = {response.StatusCode.ToString()}, {response.ReasonPhrase}");
                    return EConnectionResult.InvalidLocation;
                }

                //login to the server and receive a securityToken which is used for future communications.
                var serverResponse = await response.Content.ReadAsStringAsync();
                if (string.IsNullOrEmpty(serverResponse))
                {
                    if (!retryStarted)
                        _logger.LogCritical(4, $"No response returned connecting with server.");
                    return EConnectionResult.InvalidLocation;
                }

                JObject parsedServerResponse;
                try
                {
                    parsedServerResponse = JObject.Parse(serverResponse);
                }
                catch (JsonException)
                {
                    if (!retryStarted)
                        _logger.LogCritical(4,
                            $"An invalid response was returned connecting with server.  Response was: \"{serverResponse}\".");
                    return EConnectionResult.InvalidLocation;
                }

                if ((bool) parsedServerResponse["success"])
                {
                    var securityToken = (string) parsedServerResponse["securityToken"];
                    var userToken = (string) parsedServerResponse["userToken"];
                    var ipAddress = (string) parsedServerResponse["ipAddress"];
                    // var userHash = (string)parsedServerResponse["userHash"];

                    RemoteSettings.Runtime.ExternalIpAddress = ipAddress;

                    if (RemoteSettings.Runtime.SaveSettings)
                    {
                        // if login via password, then store the returned authentication toke
                        if (!string.IsNullOrEmpty(RemoteSettings.Runtime.Password))
                        {
                            RemoteSettings.AppSettings.UserToken = userToken;
                        }

                        //create a temporary settings file that does not contain the RunTime property.
                        var tmpSettings = new RemoteSettings()
                        {
                            AppSettings = RemoteSettings.AppSettings,
                            Logging = RemoteSettings.Logging,
                            SystemSettings = RemoteSettings.SystemSettings,
                            Network = RemoteSettings.Network,
                            Privacy = RemoteSettings.Privacy,
                            Permissions = RemoteSettings.Permissions,
                            NamingStandards = RemoteSettings.NamingStandards
                        };

                        File.WriteAllText(RemoteSettings.Runtime.AppSettingsPath,
                            JsonConvert.SerializeObject(tmpSettings, Formatting.Indented));
                        _logger.LogInformation(
                            "The appsettings.json file has been updated with the current settings.");


                        RemoteSettings.Runtime.SaveSettings = false;

                    }

                    SecurityToken = securityToken;

                    _logger.LogInformation(2, "User authentication successful.");
                    return EConnectionResult.Connected;
                }

                _logger.LogCritical(3,
                    "User authentication failed.  Run with the -reset flag to update the settings.  The authentication message from the server was: {0}.",
                    parsedServerResponse["message"].ToString());
                return EConnectionResult.InvalidCredentials;
            }
            catch (Exception ex)
            {
                _logger.LogError(10, ex, "Error logging in: " + ex.Message);
                return EConnectionResult.UnhandledException;
            }
        }



    }

}