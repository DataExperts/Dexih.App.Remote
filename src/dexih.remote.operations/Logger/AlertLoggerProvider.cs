using dexih.operations.Alerts;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations.Logger
{
    public class AlertLoggerProvider : ILoggerProvider
    {
        private readonly IAlertQueue _alertQueue;
        
        public AlertLoggerProvider(IAlertQueue alertQueue)
        {
            _alertQueue = alertQueue;
        }
        /// <inheritdoc />
        public ILogger CreateLogger(string name)
        {
            return new AlertLogger(_alertQueue);
        }

        public void Dispose()
        {
        }
    }
}