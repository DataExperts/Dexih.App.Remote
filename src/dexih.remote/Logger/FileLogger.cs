//using System;
//using System.Text;
//using Microsoft.Extensions.Logging;
//using Microsoft.Extensions.Logging.Abstractions.Internal;
//using Microsoft.Extensions.Logging.Console;
//using Microsoft.Extensions.Logging.Console.Internal;
//
//namespace dexih.remote
//{
//    public class FileLogger: ILogger
//    {
//       private static readonly string _loglevelPadding = ": ";
//        private static readonly string _messagePadding;
//        private static readonly string _newLineWithMessagePadding;
//
//        private readonly FileLoggerProcessor _queueProcessor;
//        private Func<string, LogLevel, bool> _filter;
//
//        [ThreadStatic]
//        private static StringBuilder _logBuilder;
//
//        static FileLogger()
//        {
//            var logLevelString = GetLogLevelString(LogLevel.Information);
//            _messagePadding = new string(' ', logLevelString.Length + _loglevelPadding.Length);
//            _newLineWithMessagePadding = Environment.NewLine + _messagePadding;
//        }
//
//        public FileLogger(string name, Func<string, LogLevel, bool> filter, bool includeScopes, string path, string fileName = null)
//            : this(name, filter, includeScopes ? new LoggerExternalScopeProvider() : null, new FileLoggerProcessor(path, fileName))
//        {
//        }
//
//        public FileLogger(string name, Func<string, LogLevel, bool> filter, IExternalScopeProvider scopeProvider, string path, string fileName = null)
//            : this(name, filter, scopeProvider, new FileLoggerProcessor(path, fileName))
//        {
//        }
//
//        internal FileLogger(string name, Func<string, LogLevel, bool> filter, IExternalScopeProvider scopeProvider, FileLoggerProcessor loggerProcessor)
//        {
//            Name = name ?? throw new ArgumentNullException(nameof(name));
//            Filter = filter ?? ((category, logLevel) => true);
//            ScopeProvider = scopeProvider;
//            _queueProcessor = loggerProcessor;
//        }
//
//
//
//        public Func<string, LogLevel, bool> Filter
//        {
//            get => _filter;
//            set => _filter = value ?? throw new ArgumentNullException(nameof(value));
//        }
//
//        public string Name { get; }
//
//        [Obsolete("Changing this property has no effect. Use " + nameof(ConsoleLoggerOptions) + "." + nameof(ConsoleLoggerOptions.IncludeScopes) + " instead")]
//        public bool IncludeScopes { get; set; }
//
//        internal IExternalScopeProvider ScopeProvider { get; set; }
//
//
//        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
//        {
//            if (!IsEnabled(logLevel))
//            {
//                return;
//            }
//
//            if (formatter == null)
//            {
//                throw new ArgumentNullException(nameof(formatter));
//            }
//
//            var message = formatter(state, exception);
//
//            if (!string.IsNullOrEmpty(message) || exception != null)
//            {
//                WriteMessage(logLevel, Name, eventId.Id, message, exception);
//            }
//        }
//
//        public virtual void WriteMessage(LogLevel logLevel, string logName, int eventId, string message, Exception exception)
//        {
//            var logBuilder = _logBuilder;
//            _logBuilder = null;
//
//            if (logBuilder == null)
//            {
//                logBuilder = new StringBuilder();
//            }
//
//            var logLevelString = string.Empty;
//
//            // Example:
//            // INFO: ConsoleApp.Program[10]
//            //       Request received
//
//            logLevelString = GetLogLevelString(logLevel);
//            // category and event id
//            
////            logBuilder.Append(_loglevelPadding);
////            logBuilder.Append(logName);
////            logBuilder.Append("[");
////            logBuilder.Append(eventId);
////            logBuilder.AppendLine("]");
//
//            // scope information
//            GetScopeInformation(logBuilder);
//
//            if (!string.IsNullOrEmpty(message))
//            {
//                // message
//                logBuilder.Append(" ");
//                logBuilder.Append(DateTime.Now.ToString("g") + " ");
//
//                var len = logBuilder.Length;
//                logBuilder.AppendLine(message);
//                logBuilder.Replace(Environment.NewLine, _newLineWithMessagePadding, len, message.Length);
//            }
//
//            // Example:
//            // System.InvalidOperationException
//            //    at Namespace.Class.Function() in File:line X
//            if (exception != null)
//            {
//                // exception message
//                logBuilder.AppendLine(exception.ToString());
//            }
//
//            if (logBuilder.Length > 0)
//            {
//                var hasLevel = !string.IsNullOrEmpty(logLevelString);
//                // Queue log message
//                _queueProcessor.EnqueueMessage(new LogMessageEntry()
//                {
//                    Message = logBuilder.ToString(),
//                    LevelString = hasLevel ? logLevelString : null,
//                });
//            }
//
//            logBuilder.Clear();
//            if (logBuilder.Capacity > 1024)
//            {
//                logBuilder.Capacity = 1024;
//            }
//            _logBuilder = logBuilder;
//        }
//
//        public bool IsEnabled(LogLevel logLevel)
//        {
//            if (logLevel == LogLevel.None)
//            {
//                return false;
//            }
//
//            return Filter(Name, logLevel);
//        }
//
//        public IDisposable BeginScope<TState>(TState state) => ScopeProvider?.Push(state) ?? NullScope.Instance;
//
//        private static string GetLogLevelString(LogLevel logLevel)
//        {
//            switch (logLevel)
//            {
//                case LogLevel.Trace:
//                    return "trce";
//                case LogLevel.Debug:
//                    return "dbug";
//                case LogLevel.Information:
//                    return "info";
//                case LogLevel.Warning:
//                    return "warn";
//                case LogLevel.Error:
//                    return "fail";
//                case LogLevel.Critical:
//                    return "crit";
//                default:
//                    throw new ArgumentOutOfRangeException(nameof(logLevel));
//            }
//        }
//
//        private void GetScopeInformation(StringBuilder stringBuilder)
//        {
//            var scopeProvider = ScopeProvider;
//            if (scopeProvider != null)
//            {
//                var initialLength = stringBuilder.Length;
//
//                scopeProvider.ForEachScope((scope, state) =>
//                {
//                    var (builder, length) = state;
//                    var first = length == builder.Length;
//                    builder.Append(first ? "=> " : " => ").Append(scope);
//                }, (stringBuilder, initialLength));
//
//                if (stringBuilder.Length > initialLength)
//                {
//                    stringBuilder.Insert(initialLength, _messagePadding);
//                    stringBuilder.AppendLine();
//                }
//            }
//        }
//    }
//}