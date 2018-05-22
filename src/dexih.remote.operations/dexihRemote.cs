using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations;
using dexih.remote.operations;
using dexih.remote.Operations.Services;
using dexih.repository;
using Dexih.Utils.Crypto;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
        
        public enum EExitCode {
            Success = 0,
            InvalidSetting = 1,
            InvalidLogin = 2,
            Terminated = 3,
            UnknownError = 10,
            Upgrade = 20
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
        // private CookieContainer HttpCookieContainer { get; set; }

        private HttpClient _httpClient;

        private RemoteOperations _remoteOperations;

        private readonly ConcurrentDictionary<string, RemoteMessage> _responseMessages = new ConcurrentDictionary<string, RemoteMessage>(); //list of responses returned from clients.  This is updated by the hub.
        private readonly ConcurrentBag<ResponseMessage> _sendMessageQueue = new ConcurrentBag<ResponseMessage>();
        private readonly IStreams _streams = new Streams();

        /// <summary>
        /// Gets the local ip address
        /// </summary>
        /// <returns></returns>
        private string LocalIpAddress(ILogger logger)
        {
            try
            {
                using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0))
                {
                    socket.Connect("8.8.8.8", 65530);
                    if (!(socket.LocalEndPoint is IPEndPoint endPoint))
                    {
                        logger.LogError("The local network ip address could not be determined automatically.  Defaulting to local IP (127.0.0.1)");
                        return "127.0.0.1";
                    }
                    return endPoint.Address.ToString();
                }
            }
            catch (Exception ex)
            {
                logger.LogError("The local network ip address could not be determined automatically.  Defaulting to local IP (127.0.0.1)", ex);
                return "127.0.0.1";
            }
        }
        
        public DexihRemote(RemoteSettings remoteSettings, LoggerFactory loggerFactory)
        {
            LoggerFactory = loggerFactory;
            LoggerMessages = LoggerFactory.CreateLogger("Command");
            LoggerDatalinks = LoggerFactory.CreateLogger("Datalink");
            LoggerFactory.CreateLogger("Scheduler");

            _remoteSettings = remoteSettings;
            _remoteSettings.Runtime.LocalIpAddress = LocalIpAddress(LoggerMessages);

            var url = remoteSettings.AppSettings.WebServer;
            
            if (url.Substring(url.Length - 1) != "/") url += "/";
            Url = url + "api/";
			SignalrUrl = url + "remoteagent";

            SessionEncryptionKey = EncryptString.GenerateRandomKey();

            var cookies = new CookieContainer();
            var handler = new HttpClientHandler
            {
                CookieContainer = cookies
            };

            //Login to the web server to receive an authenicated cookie.
            _httpClient = new HttpClient(handler);
            
            _remoteOperations = new RemoteOperations(_remoteSettings, SessionEncryptionKey, LoggerMessages, _httpClient, Url, _streams);
        }

        public async Task<EExitCode> StartAsync(bool saveSettings, CancellationToken cancellationToken)
        {
            var logger = LoggerFactory.CreateLogger("Start");
            logger.LogInformation("Connecting to server.  ctrl-c to terminate.");

            //use this flag so the retrying only displays once.
            var retryStarted = false;
            var savedSettings = false;

            while (true)
            {
                var generateToken = !string.IsNullOrEmpty(_remoteSettings.Runtime.Password) && saveSettings;
                var loginResult = await LoginAsync(generateToken, retryStarted, cancellationToken);

                var connectResult = loginResult.connectionResult;

                if (connectResult == EConnectionResult.Connected)
                {
                    _remoteSettings.Runtime.IpAddress = loginResult.ipAddress;

                    if (!savedSettings && saveSettings)
                    {
                        _remoteSettings.AppSettings.UserToken = loginResult.userToken;
                        
                        var appSettingsFile = Directory.GetCurrentDirectory() + "/appsettings.json";
                        
                        //create a temporary settings file that does not contain the RunTime property.
                        var _tmpSettings = new RemoteSettings()
                        {
                            AppSettings = _remoteSettings.AppSettings,
                            Logging = _remoteSettings.Logging,
                            SystemSettings = _remoteSettings.SystemSettings
                        };
                        
                        File.WriteAllText(appSettingsFile, JsonConvert.SerializeObject(_tmpSettings, Formatting.Indented));
                        logger.LogInformation("The appsettings.json file has been updated with the current settings.");

                        if (!string.IsNullOrEmpty(_remoteSettings.Runtime.Password))
                        {
                            logger.LogWarning("The password is not saved to the appsettings.json file.  Create a RemoteId/UserToken combination to authenticate without a password.");
                        }

                        savedSettings = true;
                    }

                    connectResult = await ListenAsync(retryStarted, cancellationToken);
                }
                
                switch (connectResult)
                {
                    case EConnectionResult.Disconnected:
                        if (!retryStarted)
                            logger.LogWarning("Remote agent disconnected... attempting to reconnect");
                        Thread.Sleep(2000);
                        break;
                    case EConnectionResult.InvalidCredentials:
                        logger.LogWarning("Invalid credentials... terminating service.");
                        return EExitCode.InvalidLogin;
                    case EConnectionResult.InvalidLocation:
                        if (!retryStarted)
                            logger.LogWarning("Invalid location... web server might be down... retrying...");
                        Thread.Sleep(5000);
                        break;
                    case EConnectionResult.UnhandledException:
                        if (!retryStarted)
                            logger.LogWarning("Unhandled exception on remote server.. retrying...");
                        Thread.Sleep(5000);
                        break;
                }

                retryStarted = true;
            }
        }

        /// <summary>
        /// Authenticates and logs the user in
        /// </summary>
        /// <param name="generateUserToken">Create a new user authentication token</param>
        /// <param name="silentLogin"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The connection result, and a new user token if generated.</returns>
        public async Task<(EConnectionResult connectionResult, string userToken, string ipAddress, string userHash)> LoginAsync(bool generateUserToken, bool silentLogin, CancellationToken cancellationToken)
        {
            var logger = LoggerFactory.CreateLogger("Login");
            try
            {
                var runtimeVersion = Assembly.GetEntryAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

                if (!silentLogin)
                {
                    logger.LogInformation(1, $"Connecting to {Url} with user {_remoteSettings.AppSettings.User}");
                    logger.LogInformation($"This remote agent is named: \"{_remoteSettings.AppSettings.Name}\"");
                }

                _remoteSettings.Runtime.GenerateUserToken = generateUserToken;
                _remoteSettings.Runtime.Version = runtimeVersion;
                
                
                var messagesString = Json.SerializeObject(_remoteSettings, SessionEncryptionKey);
                var content = new StringContent(messagesString, Encoding.UTF8, "application/json");

                HttpResponseMessage response;
                try
                {
                    response = await _httpClient.PostAsync(Url + "Remote/Login", content, cancellationToken);
                }
                catch (HttpRequestException ex)
                {
                    if (!silentLogin)
                        logger.LogCritical(10, ex,
                            "Could not connect to the server at location: {server}, with the message: {message}",
                            Url + "/Remote/Login", ex.Message);
                    return (EConnectionResult.InvalidLocation, "", "", "");
                }
                catch (Exception ex)
                {
                    if (!silentLogin)
                        logger.LogCritical(10, ex, "Internal rrror connecting to the server at location: {0}",
                            Url + "/Remote/Login");
                    return (EConnectionResult.InvalidLocation, "", "", "");
                }

                //login to the server and receive a securityToken which is used for future communcations.
                var serverResponse = await response.Content.ReadAsStringAsync();
                if (string.IsNullOrEmpty(serverResponse))
                {
                    if (!silentLogin)
                        logger.LogCritical(4, $"No response returned connecting to {Url}.");
                    return (EConnectionResult.InvalidLocation, "", "", "");
                }

                JObject parsedServerResponse;
                try
                {
                    parsedServerResponse = JObject.Parse(serverResponse);
                }
                catch (JsonException)
                {
                    if (!silentLogin)
                        logger.LogCritical(4, $"An invalid response was returned connecting to {Url}.  Response was: \"{serverResponse}\".");
                    return (EConnectionResult.InvalidLocation, "", "", "");
                }

                if ((bool) parsedServerResponse["success"])
                {
                    SecurityToken = (string) parsedServerResponse["securityToken"];
                    var userToken = (string) parsedServerResponse["userToken"];
                    var ipAddress = (string) parsedServerResponse["ipAddress"];
                    var userHash = (string)parsedServerResponse["userHash"];
                    
                    _remoteOperations.SecurityToken = SecurityToken;

                    logger.LogInformation(2, "User authentication successful.");
                    return (EConnectionResult.Connected, userToken, ipAddress, userHash);
                }

                logger.LogCritical(3, "User authentication failed.  Run with the -reset flag to update the settings.  The authentication message from the server was: {0}.",
                    parsedServerResponse["message"].ToString());
                return (EConnectionResult.InvalidCredentials, "", "", "");
            }
            catch (Exception ex)
            {
                logger.LogError(10, ex, "Error logging in: " + ex.Message);
                return (EConnectionResult.UnhandledException, "", "", "");
            }
        }

        public async Task<EConnectionResult> ListenAsync(bool silentLogin, CancellationToken cancellationToken)
        {

            var logger = LoggerFactory.CreateLogger("Listen");
            try
            {


                using (var signalrTs = new CancellationTokenSource())
                {
                    var signalrCt = signalrTs.Token;
                    using (var linkedCTs =CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, signalrCt))
                    {
                        var ct = linkedCTs.Token;

                        var signalrConnectionResult = await SignalRConnection(signalrTs, ct);

                        if (cancellationToken.IsCancellationRequested || signalrCt.IsCancellationRequested ||
                            signalrConnectionResult != EConnectionResult.Connected)
                        {
                            return signalrConnectionResult;
                        }

                        // start the send message handler.
                        var sender = SendMessageHandler(ct);

                        //set the repeating tasks
                        TimerCallback datalinkProgressCallBack = SendDatalinkProgress;
                        var datalinkProgressTimer = new Timer(datalinkProgressCallBack, null, 500, 500);

                        _streams.OriginUrl = _remoteSettings.AppSettings.WebServer;
                        _streams.RemoteSettings = _remoteSettings;


                        // if direct upload/downloads are enabled, startup the upload/download web server.
                        if (_remoteSettings.AppSettings.DownloadDirectly)
                        {
                            try
                            {
                                var useHttps = !string.IsNullOrEmpty(_remoteSettings.AppSettings.CertificateFilename);

                                if (_remoteSettings.AppSettings.AutoGenerateCertificate)
                                {
                                    if (string.IsNullOrEmpty(_remoteSettings.AppSettings.CertificateFilename))
                                    {
                                        _remoteSettings.AppSettings.CertificateFilename = "dexih.pfx";
                                        useHttps = true;
                                    }

                                    var renew = false;

                                    if (File.Exists(_remoteSettings.AppSettings.CertificateFilename))
                                    {
                                        // Create a collection object and populate it using the PFX file
                                        using (var cert = new X509Certificate2(
                                            _remoteSettings.AppSettings.CertificateFilename,
                                            _remoteSettings.AppSettings.CertificatePassword))
                                        {
                                            var effectiveDate = DateTime.Parse(cert.GetEffectiveDateString());
                                            var expiresDate = DateTime.Parse(cert.GetExpirationDateString());

                                            // if cert expires in next 7 days, then renew.
                                            if (DateTime.Now > expiresDate.AddDays(-7))
                                            {
                                                renew = true;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        renew = true;
                                    }

                                    if (renew)
                                    {
                                        var details = new
                                        {
                                            Domain = _remoteSettings.AppSettings.DynamicDomain,
                                            Password = _remoteSettings.AppSettings.CertificatePassword
                                        };

                                        var messagesString = Json.SerializeObject(details, SessionEncryptionKey);
                                        var content = new StringContent(messagesString, Encoding.UTF8,
                                            "application/json");

                                        var response = await _httpClient.PostAsync(Url + "Remote/GenerateCertificate",
                                            content, ct);

                                        if (response.IsSuccessStatusCode)
                                        {
                                            var certificate = await response.Content.ReadAsByteArrayAsync();
                                            File.WriteAllBytes(_remoteSettings.AppSettings.CertificateFilename,
                                                certificate);
                                        }
                                        else
                                        {
                                            throw new RemoteException(
                                                $"The certificate renewal failed.  {response.ReasonPhrase}.");
                                        }
                                    }
                                }

                                if (_remoteSettings.AppSettings.EnforceHttps)
                                {

                                    if (string.IsNullOrEmpty(_remoteSettings.AppSettings.CertificateFilename))
                                    {
                                        throw new RemoteException(
                                            "The server requires https, however a CertificateFile name is not specified.");
                                    }

                                    if (!File.Exists(_remoteSettings.AppSettings.CertificateFilename))
                                    {
                                        throw new RemoteException(
                                            $"The certificate with the filename {_remoteSettings.AppSettings.CertificateFilename} does not exist.");
                                    }

                                    using (var cert = new X509Certificate2(
                                        _remoteSettings.AppSettings.CertificateFilename,
                                        _remoteSettings.AppSettings.CertificatePassword))
                                    {
                                        var effectiveDate = DateTime.Parse(cert.GetEffectiveDateString());
                                        var expiresDate = DateTime.Parse(cert.GetExpirationDateString());

                                        if (DateTime.Now < effectiveDate)
                                        {
                                            throw new RemoteException(
                                                $"The certificate with the filename {_remoteSettings.AppSettings.CertificateFilename} is not valid until {effectiveDate}.");
                                        }


                                        if (DateTime.Now > expiresDate)
                                        {
                                            throw new RemoteException(
                                                $"The certificate with the filename {_remoteSettings.AppSettings.CertificateFilename} expired on {expiresDate}.");
                                        }

                                    }

                                }


                                if (!useHttps || File.Exists(_remoteSettings.AppSettings.CertificateFilename))
                                {
                                    logger.LogInformation("Starting the data upload/download web server.");
                                    var host = new WebHostBuilder()
                                        .UseStartup<Startup>()
                                        .ConfigureServices(s => s.AddSingleton(_streams))
                                        .UseKestrel(options =>
                                        {
                                            options.Listen(IPAddress.Any,
                                                _remoteSettings.AppSettings.DownloadPort ?? 33944,
                                                listenOptions =>
                                                {
                                                    // if there is a cerficiate then default to https
                                                    if (useHttps)
                                                    {
                                                        listenOptions.UseHttps(
                                                            Path.Combine(Directory.GetCurrentDirectory(),
                                                                _remoteSettings.AppSettings.CertificateFilename),
                                                            _remoteSettings.AppSettings.CertificatePassword);
                                                    }
                                                });
                                        })
                                        .UseUrls((useHttps ? "https://*:" : "http://*:") +
                                                 _remoteSettings.AppSettings.DownloadPort)
                                        .Build();

                                    var webRunTask = host.RunAsync(ct);

                                    // remote agent will wait here until a cancel is issued.
                                    await Task.WhenAny(sender, webRunTask);
                                    host.Dispose();
                                }
                                else
                                {
                                    throw new RemoteException(
                                        $"The certificate {_remoteSettings.AppSettings.CertificateFilename} could not be found.");
                                }
                            }
                            catch (Exception e)
                            {
                                logger.LogError(10, e,
                                    $"HttpServer not started, this agent will not be able to upload/download data.");
                                await sender;
                            }
                        }
                        else
                        {
                            logger.LogWarning(10,
                                $"HttpServer not started, this agent will not be able to upload/download data.");
                            await sender;
                        }

                        datalinkProgressTimer.Dispose();
                        await HubConnection.DisposeAsync();

                        return EConnectionResult.Disconnected;
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(10, ex, "Error connecting to hub: " + ex.Message);
                return EConnectionResult.UnhandledException;
            }
        }

        /// <summary>
        /// Open a signalr connection
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task<EConnectionResult> SignalRConnection(CancellationTokenSource ts, CancellationToken ct)
        {
            var logger = LoggerFactory.CreateLogger("SignalR");

            //already a connection open then close it.
            if (HubConnection != null)
            {
                logger.LogDebug(4, "Previous websocket connection open.  Attempting to close");
                await HubConnection.DisposeAsync();
            }

            HubConnection BuildHubConnection(HttpTransportType transportType)
            {
                // create a new connection that points to web server.
                var con = new HubConnectionBuilder()
                    .WithUrl(SignalrUrl, transportType)
                    .ConfigureLogging(logging => { logging.SetMinimumLevel(_remoteSettings.Logging.LogLevel.Default); })
                    .Build();
                
                // call the "ProcessMessage" function whenever a signalr message is received.
                con.On<RemoteMessage>("Command", async message =>
                {
                    await ProcessMessage(message);
                });
                
                con.On("Abort", async () =>
                {
                    logger.LogInformation("SignalR connection aborted.");
                    await con.DisposeAsync();
                    ts.Cancel();
                });

                // when closed cancel and exit
                con.Closed += e =>
                {
                    if (e == null)
                    {
                        logger.LogInformation("SignalR connection closed.");
                    }
                    else
                    {
                        logger.LogError("SignalR connection closed with error: {0}", e.Message);
                    }
                    ts.Cancel();
                    return Task.CompletedTask;
                };

                return con;
            }
            
           
            // attempt a connection with the default transport type.
            if (!Enum.TryParse<HttpTransportType>(_remoteSettings.SystemSettings.SocketTransportType,
                out var defaultTransportType))
            {
                defaultTransportType = HttpTransportType.WebSockets;
            };

            HubConnection = BuildHubConnection(defaultTransportType);
            
            try
            {
                await HubConnection.StartAsync(ct);
            }
            catch (Exception ex)
            {
                logger.LogError(10, ex, "Failed to connect with " + defaultTransportType  + ".  Attempting longpolling.");

                // if the connection failes, attempt to downgrade to longpolling.
                if (defaultTransportType == HttpTransportType.WebSockets)
                {
                    HubConnection = BuildHubConnection(HttpTransportType.LongPolling);

                    try
                    {
                        await HubConnection.StartAsync(ct);
                    }
                    catch (Exception ex2)
                    {
                        logger.LogError(10, ex2, "Failed to connect with LongPolling.  Exiting now.");
                        return EConnectionResult.UnhandledException;
                    }
                }
                else
                {
                    logger.LogError(10, "Failed to connect with LongPolling.  Exiting now.");
                    return EConnectionResult.UnhandledException;
                }
            }

            try
            {
                await HubConnection.InvokeAsync("Connect", SecurityToken, cancellationToken: ct);
            }
            catch (HubException ex)
            {
                logger.LogError(10, ex, "Failed to call the \"Connect\" method on the server.");
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

                if (method == null)
                {
                    LoggerMessages.LogError(100, "Unknown method : " + remoteMessage.Method);
                    var error = new ReturnValue<JToken>(false, $"Unknown method: {remoteMessage.Method}.", null);
                    SendHttpResponseMessage(remoteMessage.MessageId, error);
                    return;
                }

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
                        var error = new ReturnValue<JToken>(false, $"The {remoteMessage.Method} failed.  {returnValue.Exception?.Message}", returnValue.Exception);
                        responseMessage = SendHttpResponseMessage(remoteMessage.MessageId, error);
                    }
                    else if (returnValue.IsCompleted)
                    {
                        try
                        {
                            var value = returnValue.GetType().GetProperty("Result")?.GetValue(returnValue);
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
                        _sendMessageQueue.TryTake(out var message);
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
                            "A response message failed to send to server.  Message" + returnValue.Message);
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

                        //progress messages are send and forget as it is not critical that they are received.

                        var postData = new DatalinkProgress
                        {
                            SecurityToken = SecurityToken,
                            Command = "task",
                            Results = managedTaskChanges
                        };
                        var messagesString = Json.SerializeObject(postData, SessionEncryptionKey);
                        var content = new StringContent(messagesString, Encoding.UTF8, "application/json");

                        LoggerDatalinks.LogTrace("Send task content {0}.", messagesString);

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
                            LoggerDatalinks.LogError(result.Exception, "Update task results failed.  Return message was: {0}." + result.Message);
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

    class DatalinkProgress
    {
        public string SecurityToken { get; set; }
        public string Command { get; set; }
        public IEnumerable<ManagedTask> Results { get; set; } 
    }
}
