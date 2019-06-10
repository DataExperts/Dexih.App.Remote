using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations;
using dexih.remote.Operations.Services;
using dexih.repository;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace dexih.remote.operations
{
    public class ListenerService : IHostedService
    {
        private ISharedSettings _sharedSettings;
        private IMessageQueue _messageQueue;
        private IRemoteOperations _remoteOperations;
        ILogger<ListenerService> _logger;

        private RemoteSettings _remoteSettings;

        private bool retryStarted = false;

        private HubConnection _hubConnection;
        
        public ListenerService(ISharedSettings sharedSettings, ILogger<ListenerService> logger, IMessageQueue messageQueue, IRemoteOperations remoteOperations)
        {
            _sharedSettings = sharedSettings;
            _remoteSettings = _sharedSettings.RemoteSettings;
            _messageQueue = messageQueue;
            _remoteOperations = remoteOperations;
            _logger = logger;
        }
        
        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested) { return Task.CompletedTask; }
            return Connect(cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
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
                 .ConfigureLogging(logging => { logging.SetMinimumLevel(_remoteSettings.Logging.LogLevel.Default); })
                 .Build();
            
             // call the "ProcessMessage" function whenever a signalr message is received.
             con.On<RemoteMessage>("Command", async message =>
             {
                 await ProcessMessage(message);
             });

             con.On("Abort", async () =>
             {
                 _logger.LogInformation("Listener connection aborted.");
                 _sharedSettings.ResetConnection();
                 
                 await con.StopAsync();
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
                 await _hubConnection.InvokeAsync("Connect", _sharedSettings.SecurityToken, cancellationToken: cancellationToken);
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
            // LoggerMessages.LogTrace("New Message Content: ", message);

            try
            {
                // var remoteMessage = Json.DeserializeObject<RemoteMessage>(message, TemporaryEncryptionKey);

                //if the success is false, then it is a dummy message returned through a long polling timeout, so just ignore.
                if (!remoteMessage.Success)
                    return;

                //a method with "Response" is a special case where central server is responding to agent call.  This requires no response and can be exited.
                if (remoteMessage.Method == "Response")
                {
                    _messageQueue.AddResponse(remoteMessage.MessageId, remoteMessage);
                    return;
                }

                _logger.LogDebug("Message received is command: {command}.", remoteMessage.Method);

                //JObject values = (JObject)command.Value;
                // if (!string.IsNullOrEmpty((string)command.Value))
                //     values = JObject.Parse((string)command.Value);

                var cancellationTokenSource = new CancellationTokenSource();
                
                var commandCancel = cancellationTokenSource.Token;

                var method = typeof(RemoteOperations).GetMethod(remoteMessage.Method);

                if (method == null)
                {
                    _logger.LogError(100, "Unknown method : " + remoteMessage.Method);
                    var error = new ReturnValue<JToken>(false, $"Unknown method: {remoteMessage.Method}.", null);
                    SendHttpResponseMessage(remoteMessage.MessageId, error);
                    return;
                }

                if (remoteMessage.SecurityToken == _sharedSettings.SecurityToken)
                {
                    var returnValue = method.Invoke(_remoteOperations, new object[] {remoteMessage, commandCancel});

                    if (returnValue is Task task)
                    {
                        var timeout = remoteMessage.TimeOut ?? _remoteSettings.SystemSettings.ResponseTimeout;

                        var checkTimeout = new Stopwatch();
                        checkTimeout.Start();

                        //This loop waits for task to finish with a maxtimeout of "ResponseTimeout", and send a "still running" message back every "MaxAcknowledgeWait" period.
                        while (!task.IsCompleted && checkTimeout.ElapsedMilliseconds < timeout)
                        {
                            if (await Task.WhenAny(task,
                                    Task.Delay(_remoteSettings.SystemSettings.MaxAcknowledgeWait)) == task)
                            {
                                break;
                            }

                            SendHttpResponseMessage(remoteMessage.MessageId,
                                new ReturnValue<JToken>(true, "running", null));
                        }

                        //if the task hasn't finished.  attempt to cancel and wait a small time longer.
                        if (task.IsCompleted == false)
                        {
                            cancellationTokenSource.Cancel();
                            await Task.WhenAny(task, Task.Delay(_remoteSettings.SystemSettings.CancelDelay));
                        }

                        ReturnValue responseMessage;

                        if (task.IsFaulted || task.IsCanceled)
                        {
                            ReturnValue<JToken> error;
                            if (task.Exception == null)
                            {
                                error = new ReturnValue<JToken>(false, "Unexpected error occurred.");
                            }
                            else if (task.Exception.InnerExceptions.Count == 1)
                            {
                                error = new ReturnValue<JToken>(false,
                                    $"{task.Exception.InnerExceptions[0].Message}", task.Exception);
                            }
                            else
                            {
                                error = new ReturnValue<JToken>(false, $"{task.Exception?.Message}",
                                    task.Exception);
                            }

                            responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                        }
                        else if (task.IsCompleted)
                        {
                            try
                            {
                                var value = returnValue.GetType().GetProperty("Result")?.GetValue(returnValue);
                                var jToken = Json.JTokenFromObject(value, _sharedSettings.SessionEncryptionKey);
                                responseMessage = SendHttpResponseMessage(remoteMessage.MessageId,
                                    new ReturnValue<JToken>(true, jToken));
                            }
                            catch (Exception ex)
                            {
                                var error = new ReturnValue<JToken>(false,
                                    $"The {remoteMessage.Method} failed when serializing the response message.  {ex.Message}",
                                    ex);
                                responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                            }
                        }
                        else
                        {
                            responseMessage = SendHttpResponseMessage(remoteMessage.MessageId,
                                new ReturnValue<JToken>(false,
                                    "The " + remoteMessage.Method + " failed due to a timeout.", null));
                        }

                        if (!responseMessage.Success)
                            _logger.LogError(
                                "Error occurred sending a response to the web server.  Error was: " +
                                responseMessage.Message);
                    }
                    else
                    {
                        var jToken = Json.JTokenFromObject(returnValue, _sharedSettings.SessionEncryptionKey);
                        var responseMessage = SendHttpResponseMessage(remoteMessage.MessageId,
                            new ReturnValue<JToken>(true, jToken));

                        if (!responseMessage.Success)
                            _logger.LogError(
                                "Error occurred sending a response to the web server.  Error was: " +
                                responseMessage.Message);
                    }
                }
                else
                {
                    var messageString = "The command " + remoteMessage.Method +
                                        " failed due to mismatching security tokens.";
                    SendHttpResponseMessage(remoteMessage.MessageId,
                        new ReturnValue<JToken>(false, messageString, null));
                    _logger.LogWarning(messageString);
                }
            }
            catch (TargetInvocationException ex)
            {
                _logger.LogError(100, ex, "Unknown error processing incoming message: " + ex.Message);
                var exception = ex.InnerException == null ? ex : ex.InnerException;
                var error = new ReturnValue<JToken>(false, $"{exception.Message}", ex);
                var responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                
                if (!responseMessage.Success)
                    _logger.LogError("Error occurred sending a response to the web server.  Error was: " + responseMessage.Message);
                
            }
            catch  (Exception ex)
            {
                _logger.LogError(100, ex, "Unknown error processing incoming message: " + ex.Message);
                var error = new ReturnValue<JToken>(false, $"{ex.Message}", ex);
                var responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                
                if (!responseMessage.Success)
                    _logger.LogError("Error occurred sending a response to the web server.  Error was: " + responseMessage.Message);
            }

        }
            
            private ReturnValue SendHttpResponseMessage(string messageId, ReturnValue<JToken> returnMessage)
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


    }
}