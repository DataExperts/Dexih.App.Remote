using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using dexih.transforms.Transforms;
using Dexih.Utils.Crypto;
using Dexih.Utils.ManagedTasks;

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;




namespace dexih.remote.operations
{
    public interface ISharedSettings
    {
        Task<HttpResponseMessage> PostAsync<In>(string uri, In data, CancellationToken cancellationToken);
        Task<Out> PostAsync<In, Out>(string uri, In data, CancellationToken cancellationToken);

        Task<T> GetAsync<T>(string url, CancellationToken cancellationToken);
        // Task<HttpResponseMessage> PostAsync(string uri, HttpContent content, CancellationToken cancellationToken);

        Task PostDirect(string url, string data, CancellationToken cancellationToken);
        
        string SessionEncryptionKey { get; }
        
        string InstanceId { get; set; }
        string SecurityToken { get; set; }

        RemoteSettings RemoteSettings { get; }
        
        CookieContainer CookieContainer { get; }

        Task<EConnectionResult> WaitForLogin(bool reconnect = false, CancellationToken cancellationToken = default);

        Task<RemoteLibraries> GetRemoteLibraries(CancellationToken cancellationToken);
        
        string BaseUrl { get; }

        void ResetConnection();

        Task StartDataStream(string key, Stream stream, DownloadUrl downloadUrl, string format, string fileName, bool isError, CancellationToken cancellationToken);

        void SetStream(DownloadStream downloadObject, string key);
        Task<T> GetCacheItem<T>(string key);
        void SetCacheItem<T>(string key, T value);
    }

    public class SharedSettings : ISharedSettings, IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<SharedSettings> _logger;
        private readonly IHost _host;
        private readonly IManagedTasks _managedTasks;
        private readonly IMemoryCache _memoryCache;
        private readonly IHttpClientFactory _clientFactory;
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

        public SharedSettings(IConfiguration configuration, ILogger<SharedSettings> logger, IHost host, IManagedTasks managedTasks, IMemoryCache memoryCache, IHttpClientFactory clientFactory)
        {
            _logger = logger;
            _host = host;
            _managedTasks = managedTasks;
            _memoryCache = memoryCache;
            _clientFactory = clientFactory;

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
//                var bytes = MessagePackSerializer.Serialize(data);
//                var byteContent = new ByteArrayContent(bytes);
//                byteContent.Headers.Remove("Content-Type");
//                byteContent.Headers.Add("Content-Type", "application/x-msgpack");
//                var response = await _httpClient.PostAsync(_apiUri + uri, byteContent, cancellationToken);

                var json = data.Serialize();
                var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync(_apiUri + uri, jsonContent, cancellationToken);

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
                var response = await PostAsync<In>(uri, data, cancellationToken);
                return await ProcessHttpResponse<Out>(uri, response);
        }
        
        public async Task PostDirect(string url, string data, CancellationToken cancellationToken)
        {
            await _httpClient.PostAsync(url, new StringContent(data), cancellationToken);
        }

        public async Task<T> GetAsync<T>(string url, CancellationToken cancellationToken)
        {
            var response = await _httpClient.GetAsync(url, cancellationToken);
            return await ProcessHttpResponse<T>(url, response);
        }
        
        private async Task<T> ProcessHttpResponse<T>(string uri, HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode)
            {
                switch (response.Content.Headers.ContentType.MediaType)
                {
                    case "application/json":
                        var jsonContent = await response.Content.ReadAsStringAsync();
                        var message = jsonContent.Deserialize<T>();
                        return message;
                    
                    // case "application/x-msgpack":
                    //     var result = await MessagePackSerializer.DeserializeAsync<T>(await response.Content.ReadAsStreamAsync());
                    //     return result;

                    case "text/plain":
                        if (typeof(T) == typeof(string))
                        {
                            return (T)(object) await response.Content.ReadAsStringAsync();    
                        }
                        throw new RemoteOperationException($"Http call returned plain/text however requested type was {typeof(T)}.");
                        
                    default:
                        var message2 =
                            $"Post to {uri} failed.  Unknown response type {response.Content.Headers.ContentType.MediaType}.";
                        _logger.LogError(message2);
                        throw new RemoteOperationException(message2);
                    
                }
            }
            else
            {
                var message3 = $"Post to {uri} failed.  Response: {response.ReasonPhrase}";
                _logger.LogError(message3);
                throw new RemoteOperationException(message3);
            }
        }
        

        public void ResetConnection()
        {
            _connectionStatus = EConnectionResult.Disconnected;
        }
        
        public async Task<EConnectionResult> WaitForLogin(bool reconnect = false, CancellationToken cancellationToken = default)
        {
            try
            {
                // the login semaphore only allows one login attempt simultaneously
                await _loginSemaphore.WaitAsync(cancellationToken);

                // check if we are already connected
                if (!reconnect && _connectionStatus == EConnectionResult.Connected)
                {
                    return EConnectionResult.Connected;
                }
                
                _connectionStatus = EConnectionResult.Connecting;

                // var connectionResult = EConnectionResult.Disconnected;

                var retryStarted = false;

                while (_connectionStatus != EConnectionResult.Connected)
                {
                    _connectionStatus = await LoginAsync(retryStarted, cancellationToken);
                    switch (_connectionStatus)
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
                                _logger.LogWarning($"Error:  Login returned {_connectionStatus}.. retrying...");
                            }

                            break;
                    }

                    await Task.Delay(5000, cancellationToken);
                    retryStarted = true;
                }
            }
            finally
            {
                try
                {
                    _loginSemaphore.Release();    
                }
                catch(ObjectDisposedException) {}
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
                
                if(response == null || !response.IsSuccessStatusCode)
                {
                    if (response != null)
                    {
                        var errorResponse = await response.Content.ReadAsStringAsync();
                        if (!string.IsNullOrEmpty(errorResponse))
                        {
                            _logger.LogError(errorResponse);
                        }
                    }

                    if (!retryStarted)
                        _logger.LogCritical(4,
                            $"Could not connect with server.  Status = {response?.StatusCode.ToString() ?? "Unknown"}, {response?.ReasonPhrase}");
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

                JsonElement parsedServerResponse;
                try
                {
                    parsedServerResponse = JsonDocument.Parse(serverResponse).RootElement;
                }
                catch (JsonException)
                {
                    if (!retryStarted)
                        _logger.LogCritical(4,
                            $"An invalid response was returned connecting with server.  Response was: \"{serverResponse}\".");
                    return EConnectionResult.InvalidLocation;
                }

                if (parsedServerResponse.GetProperty("success").GetBoolean())
                {
                    var instanceId = parsedServerResponse.GetProperty("instanceId").GetString();
                    var securityToken = parsedServerResponse.GetProperty("securityToken").GetString();
                    var userToken = parsedServerResponse.GetProperty("userToken").GetString();
                    var ipAddress = parsedServerResponse.GetProperty("ipAddress").GetString();
                    var defaultProxyUrl = parsedServerResponse.GetProperty("defaultProxyUrl").GetString();
                    var remoteAgentKey = parsedServerResponse.GetProperty("remoteAgentKey").GetInt64();
                    var userHash = parsedServerResponse.GetProperty("userHash").GetString();

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
                            JsonSerializer.Serialize(tmpSettings, new JsonSerializerOptions() { WriteIndented = true}));
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
                    parsedServerResponse.GetProperty("message").GetString());
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
                var globalCache = JsonExtensions.Deserialize<CacheManager>(serverResponse);
                 
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

        public async Task<T> GetCacheItem<T>(string key)
        {
            for (var i = 0; i < 50; i++)
            {
                var downloadObject = _memoryCache.Get<T>(key);
                if (downloadObject != null)
                {
                    _memoryCache.Remove(key);
                    return downloadObject;
                }

                await Task.Delay(100);
            }

            return default;
        }

        public void SetCacheItem<T>(string key, T value)
        {
            _memoryCache.Set(key, value);
        }
        
        public Task StartDataStream(string key, Stream stream, DownloadUrl downloadUrl, string format, string fileName, bool isError, CancellationToken cancellationToken)
        {
            if (downloadUrl.DownloadUrlType == EDownloadUrlType.Proxy)
            {
                var uploadUrl = $"{downloadUrl.Url}/upload/{key}/{format}/{fileName}";
                var errorUrl = $"{downloadUrl.Url}/error/{key}";
                var downloadDataTask = new PostDataTask(_httpClient, stream, uploadUrl, errorUrl, isError);
            
                var newManagedTask = new ManagedTask
                {
                    TaskId = Guid.NewGuid().ToString(),
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
                var downloadObject = new DownloadStream(fileName, format, stream, isError);
                SetStream(downloadObject, key);
            }

            return Task.CompletedTask;
        }
        
//        public async Task StartDataStream(Stream stream, string responseUrl, string messageId, string format, string fileName, CancellationToken cancellationToken)
//        {
//            if (!string.IsNullOrEmpty(responseUrl))
//            {
//                // if downloading through a proxy, start a process to upload to the proxy.
//                var startResult = await _httpClient.GetAsync($"{responseUrl}/upload/{messageId}/{format}/{fileName}", cancellationToken);
//
//                if (!startResult.IsSuccessStatusCode)
//                {
//                    throw new RemoteOperationException($"Failed to connect to the proxy server.  Message: {startResult.ReasonPhrase}");
//                }
//
//                var jsonResult = JsonDocument.Parse(await startResult.Content.ReadAsStringAsync()).RootElement;
//
//                var upload = jsonResult.GetProperty("UploadUrl").GetString();
//                var download = jsonResult.GetProperty("DownloadUrl").GetString();
//            
////                async Task UploadDataTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
////                {
////                    await _httpClient.PostAsync(upload, new StreamContent(stream), ct);
////                }
//
//                var downloadDataTask = new PostDataTask(_httpClient, stream, upload);
//            
//                var newManagedTask = new ManagedTask
//                {
//                    Reference = Guid.NewGuid().ToString(),
//                    OriginatorId = "none",
//                    Name = $"Remote Data",
//                    Category = "ProxyDownload",
//                    CategoryKey = 0,
//                    ReferenceKey = 0,
//                    ManagedObject = downloadDataTask,
//                    Triggers = null,
//                    FileWatchers = null,
//                };
//
//                _managedTasks.Add(newManagedTask);
//            }
//            else
//            {
////                SetStream();
////                // if downloading directly, then just get the stream ready for when the client connects.
////                var keys = _streams.SetDownloadStream(fileName, stream);
////                var url = $"{downloadUrl.Url}/{format}/{HttpUtility.UrlEncode(keys.Key)}/{HttpUtility.UrlEncode(keys.SecurityKey)}";
////                return url;
//            }
//        }
    }
}