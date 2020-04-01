using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


namespace dexih.remote.operations
{
    public static class HubExtension
    {
        /// <summary>
        /// Registers a handler that will be invoked when the hub method with the specified method name is invoked.
        /// </summary>
        /// <typeparam name="T1">The first argument type.</typeparam>
        /// <param name="hubConnection">The hub connection.</param>
        /// <param name="methodName">The name of the hub method to define.</param>
        /// <param name="handler">The handler that will be raised when the hub method is invoked.</param>
        /// <returns>A subscription that can be disposed to unsubscribe from the hub method.</returns>
        public static IDisposable On<T1>(
            this HubConnection hubConnection,
            string methodName,
            Func<T1, Task> func)
        {
            if (hubConnection == null)
                throw new ArgumentNullException(nameof (hubConnection));
            return hubConnection.On(methodName, new Type[1]
            {
                typeof (T1)
            }, args => func((T1) args[0]));
        }

        public static IDisposable On(
            this HubConnection hubConnection,
            string methodName,
            Func<Task> func)
        {
            if (hubConnection == null)
                throw new ArgumentNullException(nameof (hubConnection));
            return hubConnection.On(methodName, new Type[0], 
                 (args => func()));
        }
    }
    
    public class ListenerService : IHostedService
    {
        private readonly ISharedSettings _sharedSettings;
        private readonly IMessageQueue _messageQueue;
        private readonly IRemoteOperations _remoteOperations;
        private readonly ILogger<ListenerService> _logger;

        private readonly RemoteSettings _remoteSettings;
        private readonly IMemoryCache _memoryCache;
        private readonly IApplicationLifetime _applicationLifetime;

        private HubConnection _hubConnection;
        
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        
        public ListenerService(ISharedSettings sharedSettings, ILogger<ListenerService> logger, IMessageQueue messageQueue, IRemoteOperations remoteOperations, IMemoryCache memoryCache, IApplicationLifetime applicationLifetime)
        {
            _sharedSettings = sharedSettings;
            _remoteSettings = _sharedSettings.RemoteSettings;
            _messageQueue = messageQueue;
            _remoteOperations = remoteOperations;
            _logger = logger;
            _memoryCache = memoryCache;
            _applicationLifetime = applicationLifetime;
        }
        
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Listener Service is Starting.");

            if (cancellationToken.IsCancellationRequested) { return; }
            await Connect(_cancellationTokenSource.Token);

            _logger.LogInformation("Listener Service is Started.");
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _cancellationTokenSource.Cancel();

            _logger.LogInformation("Listener Service is Stopping.");
            if (_hubConnection != null)
            {
                await _hubConnection.DisposeAsync();
            }
            _logger.LogInformation("Listener Service is Stopped.");
        }

        private async Task Connect(CancellationToken cancellationToken)
        {
            _logger.LogInformation("The listener service is waiting for authentication from the server...");
            EConnectionResult connectionResult = EConnectionResult.Disconnected;

            connectionResult = await _sharedSettings.WaitForLogin(false, cancellationToken);

            if (connectionResult == EConnectionResult.Connected)
            {
                _hubConnection = BuildHubConnection(HttpTransportType.WebSockets, cancellationToken);
                connectionResult = await StartListener(cancellationToken);
                
            }

            if (connectionResult != EConnectionResult.Connected)
            {
                _logger.LogError($"Failed to start web sockets connection.  Reason: {connectionResult}.  Shutting down service.");
                _applicationLifetime.StopApplication();
            }
        }

        private DexihActiveAgent GetActiveAgent()
        {
            var activeAgent = new DexihActiveAgent()
            {
                Name = _remoteSettings.AppSettings.Name,
                IsRunning = true,
                DataPrivacyStatus = _remoteSettings.DataPrivacyStatus(),
                DownloadUrls = _remoteSettings.GetDownloadUrls(),
                IpAddress = _remoteSettings.Runtime.ExternalIpAddress,
                IsEncrypted = _remoteSettings.Network.EnforceHttps,
                InstanceId = _sharedSettings.InstanceId,
                User =  _remoteSettings.AppSettings.User,
                UpgradeAvailable = _remoteSettings.UpgradeAvailable(),
                Version = _remoteSettings.Runtime.Version,
                LatestVersion = _remoteSettings.Runtime.LatestVersion,
                LatestDownloadUrl = _remoteSettings.Runtime.LatestDownloadUrl,
                RemoteAgentKey = _remoteSettings.Runtime.RemoteAgentKey,
                NamingStandards = _remoteSettings.NamingStandards
            };

            return activeAgent;
        }
             
         private HubConnection BuildHubConnection(HttpTransportType transportType, CancellationToken cancellationToken)
         {
             var url = _sharedSettings.BaseUrl + "remoteagent";
             
             // create a new connection that points to web server.
             var con = new HubConnectionBuilder()
                 .WithUrl(url, options =>
                 {
                     options.Transports = transportType;
                     options.Cookies = _sharedSettings.CookieContainer;
                 })
                 .AddJsonProtocol(options =>
                 {
                     options.PayloadSerializerOptions.Converters.Add(new JsonObjectConverter());
                     options.PayloadSerializerOptions.Converters.Add(new JsonTimeSpanConverter());
                     options.PayloadSerializerOptions.Converters.Add(new JsonDateTimeConverter());
                 })
                 .ConfigureLogging(logging => { logging.SetMinimumLevel(_remoteSettings.Logging.LogLevel.Default); })
                 .Build();
             
//             // call the "ProcessMessage" function whenever a signalr message is received.
//             con.On<RemoteMessage>("Command", async message =>
//             {
//                 await ProcessMessage(message);
//             });
             
             //TODO Implement process message without response message.
             con.On<RemoteMessage>("Command2", async message =>
             {
                 await ProcessMessage(message);
             });

             con.On<RemoteMessage>("Response",  message =>
             {
                 _messageQueue.AddResponse(message.MessageId, message);
             });
             
             // signals the agent is alive, and sends a response message back to the calling client.
             con.On<string>("Ping", async connectionId =>
             {
                 await _hubConnection.SendAsync("Ping", GetActiveAgent(), connectionId, cancellationToken);
             });
             
             // signals the agent is alive, and sends a response message back to the calling client.
             con.On<string>("PingServer", async pingKey =>
             {
                 await _hubConnection.SendAsync("PingServer", GetActiveAgent(), pingKey, cancellationToken);
             });

             con.On<string>("Restart", async =>
             {
                 _remoteOperations.ReStart(null, cancellationToken);
                 
             });
             
             con.On("Abort", async () =>
             {
                 _logger.LogInformation("Listener connection aborted.");
                 _sharedSettings.ResetConnection();
                 
                 await con.StopAsync(cancellationToken);
             });

             // when closed cancel and exit
             con.Closed += async e =>
             {
                 _sharedSettings.ResetConnection();

                 if (e == null)
                 {
                     _logger.LogInformation("Listener connection closed.");
                 }
                 else
                 {
                     _logger.LogError("Listener connection closed with error: {0}", e.Message);
                 }

                 if (!cancellationToken.IsCancellationRequested)
                 {
                     // if closed, then attempt to reconnect.
                     await Connect(cancellationToken);
                 }
             };
        
             return con;
         }
         
         /// <summary>
         /// Open a signalr connection
         /// </summary>
         /// <param name="ts"></param>
         /// <param name="cancellationToken"></param>
         /// <returns></returns>
         private async Task<EConnectionResult> StartListener(CancellationToken cancellationToken)
         {
             try
             {
                 _logger.LogInformation("The listener service is connecting (via Websockets) with the server...");
                 await _hubConnection.StartAsync(cancellationToken);
                 // await _hubConnection.InvokeAsync("Connect", null,_sharedSettings.SecurityToken, cancellationToken: cancellationToken);
                 var activeAgent = GetActiveAgent();
                 await _hubConnection.InvokeAsync("Connect", activeAgent,_sharedSettings.SecurityToken, cancellationToken: cancellationToken);
                 _logger.LogInformation("The listener service is connected.");

             }
             catch (Exception ex)
             {
                 _logger.LogError(10, ex, "Failed to call the \"Connect\" method on the server.");
                 return EConnectionResult.UnhandledException;
             }

             return EConnectionResult.Connected;
         }
         
         
        /// <summary>
        /// Processes a message from the webserver, and redirects to the appropriate method.
        /// </summary>
        /// <param name="remoteMessage"></param>
        /// <returns></returns>
        private async Task ProcessMessage(RemoteMessage remoteMessage)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var commandCancel = cancellationTokenSource.Token;
            
            try
            {
                // var remoteMessage = Json.DeserializeObject<RemoteMessage>(message, TemporaryEncryptionKey);

                //if the success is false, then it is a dummy message returned through a long polling timeout, so just ignore.
                if (!remoteMessage.Success)
                    return;

                _logger.LogDebug($"Message received is command: {remoteMessage.Method}, messageId: {remoteMessage.MessageId}.");

                //JObject values = (JObject)command.Value;
                // if (!string.IsNullOrEmpty((string)command.Value))
                //     values = JObject.Parse((string)command.Value);

                var method = typeof(RemoteOperations).GetMethod(remoteMessage.Method);

                if (method == null)
                {
                    _logger.LogError(100, "Unknown method : " + remoteMessage.Method);
                    var error = new ReturnValue<object>(false, $"Unknown method: {remoteMessage.Method}.", null);
                    AddResponseMessage(remoteMessage.MessageId, error);
                    return;
                }

                if (remoteMessage.SecurityToken == _sharedSettings.SecurityToken)
                {
                    Stream stream;
                    
                    // if method is a task, execute async
                    if (method.ReturnType == typeof(Task))
                    {
                        var task = (Task)method.Invoke(_remoteOperations, new object[] {remoteMessage, commandCancel});
                        if (task.IsFaulted)
                        {
                            throw task.Exception;
                        }
                        await task.ConfigureAwait(false);
                        return;
                        
                    // if method is a void, execute sync
                    } else if (method.ReturnType == typeof(void))
                    {
                        method.Invoke(_remoteOperations, new object[] {remoteMessage, commandCancel});
                        return;
                    }
                    
                    // if method is a task with a return type, create a call back stream to execute
                    else if (method.ReturnType.BaseType == typeof(Task))
                    {
                        var args = method.ReturnType.GetGenericArguments();
                        if (args.Length > 0 && args[0].IsAssignableFrom(typeof(Stream)))
                        {
                            var task = (Task) method.Invoke(_remoteOperations, new object[] {remoteMessage, commandCancel});
                            if (task.IsFaulted)
                            {
                                throw task.Exception;
                            }
                            await task.ConfigureAwait(false);
                            var resultProperty = task.GetType().GetProperty("Result");
                            stream = (Stream) resultProperty.GetValue(task);
                        }
                        else
                        {
                            stream = new StreamAsyncAction<object>(async () =>
                            {
                                var task = (Task) method.Invoke(_remoteOperations, new object[] {remoteMessage, commandCancel});
                                if (task.IsFaulted)
                                {
                                    throw task.Exception;
                                }
                                await task.ConfigureAwait(false);
                                var property = task.GetType().GetProperty("Result");
                                return property.GetValue(task);

                            });
                        }

                    // if method is a stream, then start the stream.
                    } else if (method.ReturnType.IsAssignableFrom(typeof(Stream)))
                    {
                        try
                        {
                            stream = (Stream) method.Invoke(_remoteOperations,
                                new object[] {remoteMessage, commandCancel});
                        }
                        catch (TargetInvocationException ex)
                        {
                            throw ex.InnerException ?? ex;
                        }
                    }
                    
                    // other return types, execute sync.
                    else
                    {
                        try
                        {

                            stream = new StreamAction<object>(() =>
                                method.Invoke(_remoteOperations, new object[] {remoteMessage, commandCancel}));
                        }
                        catch (TargetInvocationException ex)
                        {
                            throw ex.InnerException ?? ex;
                        }
                    }
                    
                    await _sharedSettings.StartDataStream(remoteMessage.MessageId, stream, remoteMessage.DownloadUrl, "json", "", false, commandCancel);
                }
                else
                {
                    throw new RemoteException($"The command {remoteMessage.Method} failed due to mismatching security tokens.");
                }
            }
            catch  (Exception ex)
            {
                _logger.LogError(100, ex, "Unknown error processing incoming message: " + ex.Message);
                var error = new ReturnValue<object>(false, $"{ex.Message}", ex);

                // when error occurs, set local cache or send to proxy so message gets to client.
                if (remoteMessage.DownloadUrl.DownloadUrlType != EDownloadUrlType.Proxy)
                {
                    var stream = new StreamAction<ReturnValue>(() =>  error);
                    await _sharedSettings.StartDataStream(remoteMessage.MessageId, stream, remoteMessage.DownloadUrl, "json", "", true, commandCancel);
//
//                    _memoryCache.Set(remoteMessage.MessageId, stream, TimeSpan.FromSeconds(5));
                }
                else
                {
                    var result = new ReturnValue(false, ex.Message, ex);
                    await _sharedSettings.PostDirect($"{remoteMessage.DownloadUrl.Url}/error/{remoteMessage.MessageId}", result.Serialize(), commandCancel);
                }
            }
        }
        
        
            
            private ReturnValue SendHttpResponseMessage(string messageId, ReturnValue<object> returnMessage)
            {
                try
                {
                    var responseMessage = new ResponseMessage(_sharedSettings.SecurityToken, messageId, returnMessage);
                    _messageQueue.Add(responseMessage);

                    return new ReturnValue(true);
                }
                catch (Exception ex)
                {
                    return new ReturnValue(false, "Error occurred sending remote message: " + ex.Message, ex);
                }
            }

            private void AddResponseMessage(string messageId, ReturnValue<object> returnMessage)
            {
                try
                {
                    var json = returnMessage.Serialize();
                    var memoryStream = new MemoryStream();
                    var streamWriter = new StreamWriter(memoryStream);
                    streamWriter.Write(json);
                    memoryStream.Position = 0;
                    
                    var entry = _memoryCache.CreateEntry(messageId);
                    entry.SlidingExpiration = TimeSpan.FromSeconds(10);
                    entry.Value = memoryStream;

                    _memoryCache.Set(messageId, memoryStream, TimeSpan.FromSeconds(5));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred adding a response message.  Error was: " + ex.Message);
                }
            }


    }
}