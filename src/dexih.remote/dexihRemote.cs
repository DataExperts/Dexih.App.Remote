using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Diagnostics;
using System.Text;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using System.Reflection;
using dexih.operations;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using dexih.remote.operations;
using Microsoft.AspNetCore.SignalR.Client;

namespace dexih.remote
{
    public class DexihRemote
    {

        
        public enum EConnectionResult
        {
            Connected = 0,
            Disconnected = 1,
            InvalidLocation = 2,
            InvalidCredentials = 3,
            UnhandledException = 4
        }

        private readonly RemoteSettings _remoteSettings;

        private string Url { get; }
        private string SignalrUrl { get; }

        private string SecurityToken { get; set; }
        private string SessionEncryptionKey { get; }

        private LoggerFactory LoggerFactory { get; }

        private ILogger LoggerMessages { get; }
        private ILogger LoggerDatalinks { get; }

        private HubConnection HubConnection { get; set; }
        private CookieContainer HttpCookieContainer { get; set; }

        private HttpClient _httpClient;

        private RemoteOperations _remoteOperations;

        private readonly ConcurrentDictionary<string, RemoteMessage> _responseMessages = new ConcurrentDictionary<string, RemoteMessage>(); //list of responses returned from clients.  This is updated by the hub.


        private readonly ConcurrentBag<ResponseMessage> _sendMessageQueue = new ConcurrentBag<ResponseMessage>();
        
        public DexihRemote(RemoteSettings remoteSettings, LoggerFactory loggerFactory)
        {
            _remoteSettings = remoteSettings;
            LoggerFactory = loggerFactory;

            var url = remoteSettings.AppSettings.WebServer;
            
            if (url.Substring(url.Length - 1) != "/") url += "/";
            Url = url + "api/";
			SignalrUrl = url + "remoteagent";

            SessionEncryptionKey = EncryptString.GenerateRandomKey();

            LoggerMessages = LoggerFactory.CreateLogger("Command");
            LoggerDatalinks = LoggerFactory.CreateLogger("Datalink");
            LoggerFactory.CreateLogger("Scheduler");

            var encryptionIterations = 1000;
            
            var cookies = new CookieContainer();
            var handler = new HttpClientHandler()
            {
                CookieContainer = cookies
            };

            //Login to the web server to receive an authenicated cookie.
            _httpClient = new HttpClient(handler);
            
            _remoteOperations = new RemoteOperations(_remoteSettings, SessionEncryptionKey, LoggerMessages, _httpClient, Url);
        }

        /// <summary>
        /// Authenticates and logs the user in
        /// </summary>
        /// <param name="generateUserToken">Create a new user authentication token</param>
        /// <param name="silentLogin"></param>
        /// <returns>The connection result, and a new user token if generated.</returns>
        public async Task<(EConnectionResult connectionResult, string userToken)> LoginAsync(bool generateUserToken, bool silentLogin = false)
        {
            var logger = LoggerFactory.CreateLogger("Login");
            try
            {
                var runtimeVersion = Assembly.GetEntryAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

                if (!silentLogin)
                {
                    logger.LogInformation(1, "Data Experts Group - Remote Agent version {version}", runtimeVersion);
                    logger.LogInformation(1, "Connecting as {server} to url  {url} with {user}",
                        _remoteSettings.AppSettings.Name, Url, _remoteSettings.AppSettings.User);
                }

                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("User", _remoteSettings.AppSettings.User),
                    new KeyValuePair<string, string>("Password", _remoteSettings.AppSettings.Password),
                    new KeyValuePair<string, string>("UserToken", _remoteSettings.AppSettings.UserToken),
                    new KeyValuePair<string, string>("ServerName", _remoteSettings.AppSettings.Name),
                    new KeyValuePair<string, string>("EncryptionKey", SessionEncryptionKey),
                    new KeyValuePair<string, string>("RemoteAgentId", _remoteSettings.AppSettings.RemoteAgentId),
                    new KeyValuePair<string, string>("GenerateUserToken", generateUserToken.ToString()),
                    new KeyValuePair<string, string>("Version", runtimeVersion)
                });

                HttpResponseMessage response;
                try
                {
                    response = await _httpClient.PostAsync(Url + "Remote/Login", content);
                }
                catch (HttpRequestException ex)
                {
                    if (!silentLogin)
                        logger.LogCritical(10, ex,
                            "Could not connect to the server at location: {server}, with the message: {message}",
                            Url + "/Remote/Login", ex.Message);
                    return (EConnectionResult.InvalidLocation, "");
                }
                catch (Exception ex)
                {
                    if (!silentLogin)
                        logger.LogCritical(10, ex, "Internal rrror connecting to the server at location: {0}",
                            Url + "/Remote/Login");
                    return (EConnectionResult.InvalidLocation, "");
                }

                //login to the server and receive a securityToken which is used for future communcations.
                var uri = new Uri(Url);
                var serverResponse = await response.Content.ReadAsStringAsync();
                if (string.IsNullOrEmpty(serverResponse))
                {
                    logger.LogCritical(3, "No response returned from server when logging in.");
                    return (EConnectionResult.InvalidLocation, "");
                }

                var parsedServerResponse = JObject.Parse(serverResponse);

                if ((bool) parsedServerResponse["success"])
                {
                    SecurityToken = (string) parsedServerResponse["securityToken"];
                    var userToken = (string) parsedServerResponse["userToken"];
                    _remoteOperations.SecurityToken = SecurityToken;

                    logger.LogInformation(2, "User authentication successful.");
                    return (EConnectionResult.Connected, userToken);
                }
                else
                {
                    logger.LogCritical(3, "User authentication failed.  Run with the -reset flag to update the settings.  The error was message: {0}.",
                        parsedServerResponse?["message"].ToString());
                    return (EConnectionResult.InvalidCredentials, "");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(10, ex, "Error logging in: " + ex.Message);
                return (EConnectionResult.UnhandledException, "");
            }
        }

        public async Task<EConnectionResult> ListenAsync(bool silentLogin = false)
        {

            var logger = LoggerFactory.CreateLogger("Listen");
            try
            {
                if (HubConnection != null)
                {
                    logger.LogDebug(4, "Previous websocket connection open.  Attempting to close");
                    await HubConnection.DisposeAsync();
                }

                // Task listener;

                var ts = new CancellationTokenSource();
                var ct = ts.Token;

                //Connect to the server.
                //var responseCookies = handler.CookieContainer.GetCookies(uri);

                HubConnection BuildHubConnection(Microsoft.AspNetCore.Sockets.TransportType transportType)
                {
                    var con = new HubConnectionBuilder()
                        .WithUrl(SignalrUrl)
                        .WithLoggerFactory(LoggerFactory)
                        .WithTransport(transportType)
                        .Build();

                    con.On<RemoteMessage>("Command", async (message) =>
                    {
                        await ProcessMessage(message);
                    });

                    con.Closed += e =>
                    {
                        logger.LogError("Connection close with error: {0}", e);
                        ts.Cancel();
                        return Task.CompletedTask;
                    };

                    return con;
                }


                HubConnection = BuildHubConnection(Microsoft.AspNetCore.Sockets.TransportType.WebSockets);
                
                try
                {
                    await HubConnection.StartAsync();
                }
                catch (Exception ex)
                {
                    logger.LogError(10, ex, "Failed to connect with websockets.  Attempting longpolling.");
                    HubConnection = BuildHubConnection(Microsoft.AspNetCore.Sockets.TransportType.LongPolling);
                    await HubConnection.StartAsync();
                }
                
				await HubConnection.InvokeAsync<bool>("Connect", SecurityToken, cancellationToken: ct);

                var sender = SendMessageHandler(ct);

                //set the repeating tasks
                TimerCallback datalinkProgressCallBack = SendDatalinkProgress;
                var datalinkProgressTimer = new Timer(datalinkProgressCallBack, null, 500, 500);

                // remote agent will wait here until a cancel is issued.
                await sender;
                
                datalinkProgressTimer.Dispose();
				await HubConnection.DisposeAsync();

                return EConnectionResult.Disconnected;
            }
            catch (Exception ex)
            {
                logger.LogError(10, ex, "Error connecting to hub: " + ex.Message);
                return EConnectionResult.UnhandledException;
            }
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
                if(remoteMessage.Method == "Response")
                {
                    _responseMessages.TryAdd(remoteMessage.MessageId, remoteMessage);
                    return;
                }

                LoggerMessages.LogDebug("Message recieved is command: {command}.", remoteMessage.Method);

                //JObject values = (JObject)command.Value;
                // if (!string.IsNullOrEmpty((string)command.Value))
                //     values = JObject.Parse((string)command.Value);

                var cancellationTokenSource = new CancellationTokenSource();
                var commandCancel = cancellationTokenSource.Token;

                var method = typeof(RemoteOperations).GetMethod(remoteMessage.Method);

                var returnValue = (Task)method.Invoke(_remoteOperations, new object[] { remoteMessage, commandCancel });

                if (remoteMessage.SecurityToken == SecurityToken)
                {
					var timeout = remoteMessage.TimeOut ?? _remoteSettings.SystemSettings.ResponseTimeout;

                    var checkTimeout = new Stopwatch();
                    checkTimeout.Start();

                    //This loop waits for task to finish with a maxtimeout of "ResponseTimeout", and send a "still running" message back every "MaxAcknowledgeWait" period.
                    while (!returnValue.IsCompleted && checkTimeout.ElapsedMilliseconds < timeout)
                    {
                        if (await Task.WhenAny(returnValue, Task.Delay(_remoteSettings.SystemSettings.MaxAcknowledgeWait)) == returnValue)
                        {
                            break;
                        }
                        SendHttpResponseMessage(remoteMessage.MessageId, new ReturnValue<JToken>(true, "running", null));
                    }

                    //if the task hasn't finished.  attempt to cancel and wait a small time longer.
                    if (returnValue.IsCompleted == false)
                    {
                        cancellationTokenSource.Cancel();
                        await Task.WhenAny(returnValue, Task.Delay(_remoteSettings.SystemSettings.CancelDelay));
                    }

                    ReturnValue responseMessage;

                    if (returnValue.IsFaulted || returnValue.IsCanceled)
                    {
                        var error = new ReturnValue<JToken>(false, $"The {remoteMessage.Method} failed.  {returnValue.Exception.Message}", returnValue.Exception);
                        responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                    }
                    else if (returnValue.IsCompleted)
                    {
                        try
                        {
                            var value = returnValue.GetType().GetProperty("Result").GetValue(returnValue);
                            var jToken = Json.JTokenFromObject(value, SessionEncryptionKey);
                            responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, new ReturnValue<JToken>(true, jToken));
                        }
                        catch(Exception ex)
                        {
                            var error = new ReturnValue<JToken>(false, $"The {remoteMessage.Method} failed when serializing the response message.  {ex.Message}", ex);
                            responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                        }
                    }
                    else
                    {
                        responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, new ReturnValue<JToken>(false, "The " + remoteMessage.Method + " failed due to a timeout.", null));
                    }

                    if (!responseMessage.Success)
                        LoggerMessages.LogError("Error occurred sending a response to the web server.  Error was: " + responseMessage.Message);
                }
                else
                {
                    var messageString = "The command " + remoteMessage.Method + " failed due to mismatching security tokens.";
                    SendHttpResponseMessage(remoteMessage.MessageId, new ReturnValue<JToken>(false, messageString, null));
                    LoggerMessages.LogWarning(messageString);
                }
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(100, ex, "Unknown error processing incoming message: " + ex.Message);
            }

        }

        /// <summary>
        /// Sends messages (such as datalink progress) in batch every 500ms.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task SendMessageHandler(CancellationToken cancellationToken)
        {
            var logger = LoggerFactory.CreateLogger("SendMessageHandler");
            
            
            while (true)
            {
                if (_sendMessageQueue.Count > 0)
                {
                    var messages = new List<ResponseMessage>();

                    while (_sendMessageQueue.Count > 0)
                    {
                        var success = _sendMessageQueue.TryTake(out var message);
                        messages.Add(message);
                    }

                    var messagesString = Json.SerializeObject(messages, SessionEncryptionKey);
                    var content = new StringContent(messagesString, Encoding.UTF8, "application/json");

                    var response = await _httpClient.PostAsync(Url + "Remote/UpdateResponseMessage", content, cancellationToken);
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    
                    var returnValue =
                        Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(),
                            SessionEncryptionKey);
                    
                    if (!returnValue.Success)
                    {
                        logger.LogError(1, returnValue.Exception,
                            "A responsemessage failed to send to server.  Message" + returnValue.Message);
                    }
                }

                await Task.Delay(500, cancellationToken);
            }
        }

        private ReturnValue SendHttpResponseMessage(string messageId, ReturnValue<JToken> returnMessage)
        {
            try
            {
                var responseMessage = new ResponseMessage(SecurityToken, messageId, returnMessage);
                _sendMessageQueue.Add(responseMessage);

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "Error occurred sending remote message: " + ex.Message, ex);
            }
        }

     private bool _sendDatalinkProgressBusy;

        /// <summary>
        /// Sends the progress and status of any datalinks to the central server.
        /// </summary>
        /// <param name="stateInfo"></param>
        private async void SendDatalinkProgress(object stateInfo)
        {
            try
            {
                if (!_sendDatalinkProgressBusy)
                {
                    _sendDatalinkProgressBusy = true;

                    if (_remoteOperations.TaskChangesCount() > 0)
                    {
                        var managedTaskChanges = _remoteOperations.GetTaskChanges(true);
                        var results = Json.SerializeObject(managedTaskChanges, SessionEncryptionKey);

                        //progress messages are send and forget as it is not critical that they are received.
                        var content = new FormUrlEncodedContent(new[]
                        {
                            new KeyValuePair<string, string>("RemoteToken", SecurityToken),
                            new KeyValuePair<string, string>("Command", "task"),
                            new KeyValuePair<string, string>("Results", results)
                        });

                        var start = new Stopwatch();
                        start.Start();
                        var response = await _httpClient.PostAsync(Url + "Remote/UpdateTasks", content);
                        start.Stop();
                        LoggerDatalinks.LogDebug("Send task results: http Post to {0}." + Url + "Remote/UpdateTasks");
                        LoggerDatalinks.LogTrace("Send task results completed in {0}ms.", start.ElapsedMilliseconds);

                        var responseContent = await response.Content.ReadAsStringAsync();

                        var result = Json.DeserializeObject<ReturnValue>(responseContent, SessionEncryptionKey);

                        if (result.Success == false)
                        {
                            LoggerDatalinks.LogError("Update task results failed.  Return message was: {0}." + result.Message);
                        }
                    }

                    _sendDatalinkProgressBusy = false;
                }
            }
            catch (Exception ex)
            {
                LoggerDatalinks.LogError(250, ex, "Send datalink progress failed with error.  Error was: {0}." + ex.Message);
                _sendDatalinkProgressBusy = false;
            }
        }
    }
}
