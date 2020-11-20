using System;
using dexih.operations.Alerts;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations.Logger
{
    public class AlertLogger : ILogger
    {
        private readonly IAlertQueue _alertQueue;

        /// <summary>
        /// Initializes a new instance of the <see cref="DebugLogger"/> class.
        /// </summary>
        public AlertLogger(IAlertQueue alertQueue)
        {
            _alertQueue = alertQueue;
        }

        /// <inheritdoc />
        public IDisposable BeginScope<TState>(TState state)
        {
            return NullScope.Instance;
        }

        /// <inheritdoc />
        public bool IsEnabled(LogLevel logLevel)
        {
            // If the filter is null, everything is enabled
            // unless the debugger is not attached
            return _alertQueue != null && logLevel == LogLevel.Critical;
        }

        /// <inheritdoc />
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            if (formatter == null)
            {
                throw new ArgumentNullException(nameof(formatter));
            }

            var message = formatter(state, exception);

            if (string.IsNullOrEmpty(message))
            {
                return;
            }

            message = $"{ logLevel }: {message}";

            if (exception != null)
            {
                message += Environment.NewLine + Environment.NewLine + exception;
            }

            var alert = new Alert()
            {
                Subject = "Critical Error",
                Body = "The following critical error was logged on the remote agent: \n\n" + message
            };

            _alertQueue.Add(alert);
        }
    }
}