using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
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
                    await _messageQueue.WaitForMessage(cancellationToken);

                    if (_messageQueue.Count > 0)
                    {
                        var messages = new List<ResponseMessage>();

                        while (_messageQueue.Count > 0)
                        {
                            _messageQueue.TryDeque(out var message);
                            messages.Add(message);
                        }

                        var returnValue =
                            await _sharedSettings.PostAsync<List<ResponseMessage>, ReturnValue>(
                                "Remote/UpdateResponseMessage", messages,
                                cancellationToken);

                        if (!returnValue.Success)
                        {
                            _logger.LogError(1, returnValue.Exception,
                                "A response message failed to send to server.  Message" + returnValue.Message);
                        }
                    }

                    await Task.Delay(500, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"The message service encountered the following error: {ex.Message}");
                }
            }
            
            _logger.LogInformation("Message service has stopped.");
        }

        private void TaskProgressChange(ManagedTask value, ManagedTaskProgressItem progressItem)
        {
            // run as a separate task to minimise delays to core processes.
            if (_sendDatalinkProgress == null || _sendDatalinkProgress.IsCompleted)
            {
                _sendDatalinkProgress = SendDatalinkProgress();
            }
        }

        private void TaskStatusChange(ManagedTask value, EManagedTaskStatus managedTaskStatus)
        {
            if (managedTaskStatus == EManagedTaskStatus.Error)
            {
                _logger.LogWarning(value.Exception, $"The task {value.Name} with referenceId {value.ReferenceId} reported an error: {value.Message} ");    
            }

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
                            Command = EMessageCommand.Task,
                            Results = managedTaskChanges.ToList()
                        };

                        var start = Stopwatch.StartNew();
                        var result = await _sharedSettings.PostAsync<DatalinkProgress, ReturnValue>("Remote/UpdateTasks", postData, CancellationToken.None);
                        start.Stop();

                        _logger.LogTrace("Send task results completed in {0}ms.", start.ElapsedMilliseconds);

                        if (result == null )
                        {
                            _logger.LogError("Update task returned no message");
                        }
                        else if (result.Success == false)
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
                    "Send datalink progress failed with error.  Error was: {0}.", ex.Message);
                _sendDatalinkProgressBusy = false;
            }
        }
    }
}