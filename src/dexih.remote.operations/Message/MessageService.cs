using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations;
using Dexih.Utils.Crypto;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.Operations.Services
{
    public class MessageService : BackgroundService
    {
        private readonly ILogger<MessageService> _logger;
        private readonly IMessageQueue _messageQueue;
        private readonly ISharedSettings _sharedSettings;
        private readonly IManagedTasks _managedTasks;

        private Task _sendDatalinkProgress;

        public MessageService(
            ILogger<MessageService> logger, IMessageQueue messageQueue, ISharedSettings sharedSettings,
            IManagedTasks managedTasks)
        {
            _logger = logger;
            _messageQueue = messageQueue;
            _sharedSettings = sharedSettings;
            _managedTasks = managedTasks;

            managedTasks.OnProgress += TaskProgressChange;
            managedTasks.OnStatus += TaskStatusChange;

        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Message Service is starting.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await _messageQueue.WaitForMessage();

                    if (_messageQueue.Count > 0)
                    {
                        var messages = new List<ResponseMessage>();

                        while (_messageQueue.Count > 0)
                        {
                            _messageQueue.TryDeque(out var message);
                            messages.Add(message);
                        }

                        var response =
                            await _sharedSettings.PostAsync("Remote/UpdateResponseMessage", messages,
                                cancellationToken);
                        if (cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }

                        var returnValue =
                            Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(),
                                _sharedSettings.SessionEncryptionKey);

                        if (!returnValue.Success)
                        {
                            _logger.LogError(1, returnValue.Exception,
                                "A response message failed to send to server.  Message" + returnValue.Message);
                        }
                    }
                    await Task.Delay(500, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"The message service encountered the following error: {ex.Message}");
                }
            }
            
            _logger.LogInformation("Message service has stopped.");
        }

        private void TaskProgressChange(object value, ManagedTaskProgressItem progressItem)
        {
            // run as a separate task to minimise delays to core processes.
            if (_sendDatalinkProgress == null || _sendDatalinkProgress.IsCompleted)
            {
                _sendDatalinkProgress = SendDatalinkProgress();
            }
        }

        private void TaskStatusChange(object value, EManagedTaskStatus managedTaskStatus)
        {
            // run as a separate task to minimise delays to core processes.
            if (_sendDatalinkProgress == null || _sendDatalinkProgress.IsCompleted)
            {
                _sendDatalinkProgress = SendDatalinkProgress();
            }
        }

        private bool _sendDatalinkProgressBusy;

        /// <summary>
        /// Sends the progress and status of any datalinks to the central server.
        /// </summary>
        private async Task SendDatalinkProgress()
        {
            try
            {
                if (!_sendDatalinkProgressBusy)
                {
                    _sendDatalinkProgressBusy = true;

                    while (_managedTasks.TaskChangesCount() > 0)
                    {
                        var managedTaskChanges = _managedTasks.GetTaskChanges(true);

                        //progress messages are send and forget as it is not critical that they are received.

                        var postData = new DatalinkProgress
                        {
                            InstanceId = _sharedSettings.InstanceId,
                            SecurityToken = _sharedSettings.SecurityToken,
                            Command = "task",
                            Results = managedTaskChanges.ToList()
                        };

                        var start = Stopwatch.StartNew();
                        var response = await _sharedSettings.PostAsync("Remote/UpdateTasks", postData, CancellationToken.None);
                        start.Stop();

                        _logger.LogTrace("Send task results completed in {0}ms.", start.ElapsedMilliseconds);

                        var responseContent = await response.Content.ReadAsStringAsync();

                        var result = Json.DeserializeObject<ReturnValue>(responseContent, _sharedSettings.SessionEncryptionKey);

                        if (result.Success == false)
                        {
                            _logger.LogError(result.Exception,
                                "Update task results failed.  Return message was: " + result.Message);
                        }

                        // wait a little while for more tasks results to arrive.
                        await Task.Delay(500);
                    }

                    _sendDatalinkProgressBusy = false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(250, ex,
                    "Send datalink progress failed with error.  Error was: {0}." + ex.Message);
                _sendDatalinkProgressBusy = false;
            }
        }
    }

    internal class DatalinkProgress
    {
        public string InstanceId { get; set; }
        public string SecurityToken { get; set; }
        public string Command { get; set; }
        public IEnumerable<ManagedTask> Results { get; set; } 
    }
}