using System;
using System.Net;
using System.Net.Mail;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations.Alerts;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    public class AlertService : BackgroundService
    {
        private readonly ILogger<AlertService> _logger;
        private readonly IAlertQueue _alertQueue;
        private readonly ISharedSettings _sharedSettings;
        private readonly IManagedTasks _managedTasks;
        private readonly SmtpClient _smtpClient;
        private readonly string[] _adminEmails;
        private readonly string _footer;

        private Task _sendDatalinkProgress;

        public AlertService(
            ILogger<AlertService> logger, IAlertQueue alertQueue, ISharedSettings sharedSettings)
        {
            _logger = logger;
            _alertQueue = alertQueue;
            _sharedSettings = sharedSettings;
            _adminEmails = sharedSettings.RemoteSettings.Alerts.AdminEmails;

            if (sharedSettings.RemoteSettings?.Alerts?.SmtpServer != null)
            {
                _smtpClient = new SmtpClient(sharedSettings.RemoteSettings.Alerts.SmtpServer)
                {
                    UseDefaultCredentials = false,
                    Port = sharedSettings.RemoteSettings.Alerts.SmtpPort,
                    EnableSsl = sharedSettings.RemoteSettings.Alerts.EnableSsl,
                    
                    Credentials = new NetworkCredential(sharedSettings.RemoteSettings.Alerts.SmtpUserName,
                        sharedSettings.RemoteSettings.Alerts.SmtpPassword),
                };

                _smtpClient.SendCompleted += (s, e) =>
                {
                    if (e.Cancelled)
                    {
                        _logger.LogError("The email message was cancelled.");
                    }

                    if (e.Error != null)
                    {
                        _logger.LogError(e.Error, $"There was an error sending an alert email.");
                    }
                };

                
                _footer = $"Remote Agent Name: {_sharedSettings.RemoteSettings.AppSettings.Name}";

            }
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            if (_sharedSettings.RemoteSettings.Alerts.AlertOnShutdown)
            {
                _alertQueue.Add(new Alert()
                {
                    Subject = "Remote agent has started.",
                    Body = "The remote agent has successfully started."
                });
            }

            if (_smtpClient == null)
            {
                _logger.LogWarning("Alert Service could not be started.");
                return;
            }

            _logger.LogInformation("Alert Service is starting.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await _alertQueue.WaitForMessage(cancellationToken);

                    while (_alertQueue.Count > 0)
                    {
                        _alertQueue.TryDeque(out var alert);

                        await SendMessage(alert, cancellationToken);
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
            
            if (_sharedSettings.RemoteSettings.Alerts.AlertOnShutdown)
            {
                var alert = new Alert()
                {
                    Subject = "Remote agent has shutdown.",
                    Body = "The remote agent has shutdown."
                };

                await SendMessage(alert, CancellationToken.None);
            }
            
            _logger.LogInformation("Message service has stopped.");
        }

        private Task SendMessage(Alert alert, CancellationToken cancellationToken)
        {
            var mailMessage = new MailMessage
            {
                Subject = alert.Subject,
                Body = alert.Body + "\n\n" + _footer,
                From = new MailAddress(_sharedSettings.RemoteSettings.Alerts.FromEmail, "Remote Agent (do not reply)")
            };

            foreach (var email in _adminEmails)
            {
                mailMessage.Bcc.Add(email);
            }

            if(alert.Emails != null)
            {
                foreach (var email in alert.Emails)
                {
                    mailMessage.To.Add(email);
                }
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                return _smtpClient.SendMailAsync(mailMessage);
            }
            
            return Task.CompletedTask;
        }
      
    }
}