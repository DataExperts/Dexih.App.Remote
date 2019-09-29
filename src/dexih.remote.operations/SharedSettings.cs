using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using dexih.transforms.Transforms;
using Dexih.Utils.Crypto;
using Dexih.Utils.ManagedTasks;
using MessagePack;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace dexih.remote.operations
{
    public interface ISharedSettings
    {
        Task<HttpResponseMessage> PostAsync<In>(string uri, In data, CancellationToken cancellationToken);
        Task<Out> PostAsync<In, Out>(string uri, In data, CancellationToken cancellationToken);
        Task<HttpResponseMessage> PostAsync(string uri, HttpContent content, CancellationToken cancellationToken);

        string SessionEncryptionKey { get; }
        
        string InstanceId { get; set; }
        string SecurityToken { get; set; }

        RemoteSettings RemoteSettings { get; }
        
        CookieContainer CookieContainer { get; }

        Task<EConnectionResult> WaitForLogin(bool reconnect = false, CancellationToken cancellationToken = default);

        Task<RemoteLibraries> GetRemoteLibraries(CancellationToken cancellationToken);
        
        string BaseUrl { get; }

        bool CompleteUpgrade { get; set; }
        void ResetConnection();

        Task StartDataStream(string key, Stream stream, bool useProxy, string format, string fileName,
            CancellationToken cancellationToken);

        void SetStream(DownloadStream downloadObject, string key);
        Task<DownloadStream> GetStream(string key);
    }

    public class SharedSettings : ISharedSettings, IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<SharedSettings> _logger;
        private readonly IHost _host;
        private readonly IManagedTasks _managedTasks;
        private readonly IMemoryCache _memoryCache;
        private readonly string _apiUri;
        private readonly SemaphoreSlim _loginSemaphore;
        private EConnectionResult _connectionStatus = EConnectionResult.Disconnected;

        private RemoteLibraries _remoteLibraries;

        public string SessionEncryptionKey { get; }

        /// <summary>
        /// Shared between the remote agent and the web server.  Ensures the connection hasn't been hijacked.
        /// </summary>
        public string SecurityToken { get; set; }
        
        /// <summary>
        /// Shared between the remote agent, web server, and clients.  Used to identify this running instance.
        /// </summary>
        public string InstanceId { get; set; }
        public RemoteSettings RemoteSettings { get; }

        public CookieContainer CookieContainer { get; }

        public string BaseUrl { get; }
        
        public bool CompleteUpgrade { get; set; }

        public SharedSettings(IConfiguration configuration, ILogger<SharedSettings> logger, IHost host, IManagedTasks managedTasks, IMemoryCache memoryCache)
        {
            _logger = logger;
            _host = host;
            _managedTasks = managedTasks;
            _memoryCache = memoryCache;

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

        public async Task<HttpResponseMessage> PostAsync<In>(string uri, In data, CancellationToken cancellationToken)
        {
            try
            {
                var bytes = MessagePackSerializer.Serialize(data);
                var byteContent = new ByteArrayContent(bytes);
                byteContent.Headers.Remove("Content-Type");
                byteContent.Headers.Add("Content-Type", "application/x-msgpack");

                var response = await _httpClient.PostAsync(_apiUri + uri, byteContent, cancellationToken);
                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error posting to server.  Url: {uri}, {ex.Message}");
                return null;
            }
        }
        
        public async Task<Out> PostAsync<In, Out>(string uri, In data, CancellationToken cancellationToken)
        {
            try
            {
                var response = await PostAsync<In>(uri, data, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    switch (response.Content.Headers.ContentType.MediaType)
                    {
                        case "application/json":
                            var jsonContent = await response.Content.ReadAsStringAsync();
                            var message = JsonConvert.DeserializeObject<Out>(jsonContent);
                            return message;
                        case "application/x-msgpack":
                            var result = await MessagePackSerializer.DeserializeAsync<Out>(await response.Content.ReadAsStreamAsync());
                            return result;
                        default:
                            _logger.LogError($"Post to {uri} failed.  Unknown response type {response.Content.Headers.ContentType.MediaType}.");
                            return default;
                    }
                }
                else
                {
                    _logger.LogError($"Post to {uri} failed.  Response: {response.ReasonPhrase}");
                }

                return default;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Post to {uri} failed.");
                return default;
            }
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
                            return EConnectionResult.InvalidCredentials;
                        case EConnectionResult.InvalidLocation:
                            if (!retryStarted)
                            {
                                _logger.LogWarning("Invalid location... web server might be down... retrying...");
                            }

                            break;
                        case EConnectionResult.Connected:
                            break;
                        default:
                            if (!retryStarted)
                            {
                                _logger.LogWarning($"Error:  Login returned {connectionResult}.. retrying...");
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

                // if no remoteAgentId specified, then create one before attempting login.
                if(string.IsNullOrEmpty(RemoteSettings.AppSettings.RemoteAgentId))
                {
                    RemoteSettings.AppSettings.RemoteAgentId = Guid.NewGuid().ToString();
                }

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
                    var instanceId = (string) parsedServerResponse["instanceId"];
                    var securityToken = (string) parsedServerResponse["securityToken"];
                    var userToken = (string) parsedServerResponse["userToken"];
                    var ipAddress = (string) parsedServerResponse["ipAddress"];
                    var defaultProxyUrl = (string) parsedServerResponse["defaultProxyUrl"];
                    var remoteAgentKey = (long) parsedServerResponse["remoteAgentKey"];
                    var userHash = (string) parsedServerResponse["userHash"];

                    RemoteSettings.Runtime.ExternalIpAddress = ipAddress;
                    RemoteSettings.Runtime.DefaultProxyUrl = defaultProxyUrl;
                    RemoteSettings.Runtime.RemoteAgentKey = remoteAgentKey;
                    RemoteSettings.Runtime.UserHash = userHash;

                    if (RemoteSettings.Runtime.SaveSettings)
                    {
                        // if login via password, then store the returned authentication toke
                        if (!string.IsNullOrEmpty(RemoteSettings.Runtime.Password))
                        {
                            RemoteSettings.AppSettings.UserToken = userToken;
                        }

                        RemoteSettings.AppSettings.UserPrompt = false;
                        
                        //create a temporary settings file that does not contain the RunTime property.
                        var tmpSettings = new RemoteSettings()
                        {
                            AppSettings = RemoteSettings.AppSettings,
                            Logging = RemoteSettings.Logging,
                            SystemSettings = RemoteSettings.SystemSettings,
                            Network = RemoteSettings.Network,
                            Privacy = RemoteSettings.Privacy,
                            Permissions = RemoteSettings.Permissions,
                            NamingStandards = RemoteSettings.NamingStandards,
                            Runtime = null
                        };

                        File.WriteAllText(RemoteSettings.Runtime.AppSettingsPath,
                            JsonConvert.SerializeObject(tmpSettings, Formatting.Indented));
                        _logger.LogInformation(
                            "The appsettings.json file has been updated with the current settings.");

                        RemoteSettings.Runtime.SaveSettings = false;

                    }

                    InstanceId = instanceId;
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

        /// <summary>
        /// Gets any libraries which are contained on the remote server, but not in the global server cache.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<RemoteLibraries> GetRemoteLibraries(CancellationToken cancellationToken)
        {
            if (_remoteLibraries != null)
            {
                return _remoteLibraries;
            }

            HttpResponseMessage response;
            try
            {
                response = await _httpClient.GetAsync(_apiUri + "Account/GetGlobalCache", cancellationToken);
            }
            catch (Exception ex)
            {
                    _logger.LogCritical(10, ex, "Internal error connecting to the server at location: {0}",
                        "Account/GetGlobalCache");
                    throw;
            }
            
            if (!response.IsSuccessStatusCode)
            {
                var message =
                    $"Could not connect with server.  Status = {response.StatusCode.ToString()}, {response.ReasonPhrase}";
                _logger.LogCritical(4, message);
                throw new Exception(message);            
            }


            try
            {
                // get the global cache from the server, and return remote libraries which are missing.
                var serverResponse = await response.Content.ReadAsStringAsync();
                var globalCache = Json.DeserializeObject<CacheManager>(serverResponse, "");
                 
                var globalFunctions = globalCache.DefaultRemoteLibraries.Functions
                    .ToDictionary(c => (c.FunctionAssemblyName, c.FunctionClassName, c.FunctionMethodName));

                var functions = Functions.GetAllFunctions().Where(c => !globalFunctions.ContainsKey((
                    c.FunctionAssemblyName, c.FunctionClassName,
                    c.FunctionMethodName))).ToList();

                var globalConnections = globalCache.DefaultRemoteLibraries.Connections
                    .ToDictionary(c => (c.ConnectionAssemblyName, c.ConnectionClassName));

                var connections = Connections.GetAllConnections().Where(c => !globalConnections.ContainsKey((
                    c.ConnectionAssemblyName, c.ConnectionClassName))).ToList();
                
                var globalTransforms = globalCache.DefaultRemoteLibraries.Transforms
                    .ToDictionary(c => (c.TransformAssemblyName, c.TransformClassName));

                var transforms = Transforms.GetAllTransforms().Where(c => !globalTransforms.ContainsKey((
                    c.TransformAssemblyName, c.TransformClassName))).ToList();
                
                
                var remoteLibraries = new RemoteLibraries()
                {
                    Functions = functions,
                    Connections = connections,
                    Transforms = transforms
                };

                _remoteLibraries = remoteLibraries;

                return remoteLibraries;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(4,
                    $"An invalid response was returned connecting with server.  Response was: \"{ex.Message}\".");
                throw;
            }
        }
        
        public void SetStream(DownloadStream downloadObject, string key)
        {
            _memoryCache.Set(key, downloadObject, TimeSpan.FromSeconds(300));
        }

        public async Task<DownloadStream> GetStream(string key)
        {
            for (var i = 0; i < 10; i++)
            {
                var downloadObject = _memoryCache.Get<DownloadStream>(key);
                if (downloadObject != null)
                {
                    return downloadObject;
                }

                await Task.Delay(100);
            }

            return null;
        }
        
        public async Task StartDataStream(string key, Stream stream, bool useProxy, string format, string fileName, CancellationToken cancellationToken)
        {
            if (useProxy)
            {
                var proxy = RemoteSettings.Network.ProxyUrl;
                if (string.IsNullOrEmpty(proxy))
                {
                    throw new RemoteException("There is no proxy server specified in appsettings.");
                }
                var uploadUrl = $"{proxy}/upload/{key}{format}/{fileName}";
                var downloadDataTask = new PostDataTask(_httpClient, stream, uploadUrl);
            
                var newManagedTask = new ManagedTask
                {
                    Reference = Guid.NewGuid().ToString(),
                    OriginatorId = "none",
                    Name = $"Remote Data",
                    Category = "ProxyDownload",
                    CategoryKey = 0,
                    ReferenceKey = 0,
                    ManagedObject = downloadDataTask,
                    Triggers = null,
                    FileWatchers = null,
                };

                _managedTasks.Add(newManagedTask);
            }
            else
            {
                // if downloading directly, then just get the stream ready for when the client connects.
                var downloadObject = new DownloadStream(fileName, format, stream);
                SetStream(downloadObject, key);
            }
        }
        
        public async Task StartDataStream(Stream stream, bool useProxy, string messageId, string format, string fileName, CancellationToken cancellationToken)
        {
            if (useProxy)
            {
                var proxy = RemoteSettings.Network.ProxyUrl;
                if (string.IsNullOrEmpty(proxy))
                {
                    throw new RemoteException("There is no proxy server specified in appsettings.");
                }
                
                // if downloading through a proxy, start a process to upload to the proxy.
                var startResult = await _httpClient.GetAsync($"{proxy}/start/{format}/{fileName}", cancellationToken);

                if (!startResult.IsSuccessStatusCode)
                {
                    throw new RemoteOperationException($"Failed to connect to the proxy server.  Message: {startResult.ReasonPhrase}");
                }

                var jsonResult = JObject.Parse(await startResult.Content.ReadAsStringAsync());

                var upload = jsonResult["UploadUrl"].ToString();
                var download = jsonResult["DownloadUrl"].ToString();
            
//                async Task UploadDataTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
//                {
//                    await _httpClient.PostAsync(upload, new StreamContent(stream), ct);
//                }

                var downloadDataTask = new PostDataTask(_httpClient, stream, upload);
            
                var newManagedTask = new ManagedTask
                {
                    Reference = Guid.NewGuid().ToString(),
                    OriginatorId = "none",
                    Name = $"Remote Data",
                    Category = "ProxyDownload",
                    CategoryKey = 0,
                    ReferenceKey = 0,
                    ManagedObject = downloadDataTask,
                    Triggers = null,
                    FileWatchers = null,
                };

                _managedTasks.Add(newManagedTask);
            }
            else
            {
//                SetStream();
//                // if downloading directly, then just get the stream ready for when the client connects.
//                var keys = _streams.SetDownloadStream(fileName, stream);
//                var url = $"{downloadUrl.Url}/{format}/{HttpUtility.UrlEncode(keys.Key)}/{HttpUtility.UrlEncode(keys.SecurityKey)}";
//                return url;
            }
        }
    }
}