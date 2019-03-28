using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console.Internal;

namespace dexih.remote
{
    [ProviderAlias("Remote")]
    public class RemoteLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, RemoteLogger> _loggers = new ConcurrentDictionary<string, RemoteLogger>();

        private readonly Func<string, LogLevel, bool> _filter;
        
        private readonly ConsoleLoggerProcessor _messageQueue = new ConsoleLoggerProcessor();

        private static readonly Func<string, LogLevel, bool> trueFilter = (cat, level) => true;
        private static readonly Func<string, LogLevel, bool> falseFilter = (cat, level) => false;
        private readonly bool _includeScopes;
        private readonly bool _disableColors;
        private IExternalScopeProvider _scopeProvider;

        public RemoteLoggerProvider(LogLevel minLevel)
            : this((category, logLevel) => logLevel >= minLevel, false, false)
        {
        }

        public RemoteLoggerProvider(Func<string, LogLevel, bool> filter, bool includeScopes, bool disableColors)
        {
            _filter = filter ?? throw new ArgumentNullException(nameof(filter));
            _includeScopes = includeScopes;
            _disableColors = disableColors;
        }

        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, CreateLoggerImplementation);
        }

        private RemoteLogger CreateLoggerImplementation(string name)
        {
            var includeScopes = _includeScopes;
            var disableColors = _disableColors;
            
            return new RemoteLogger(name, GetFilter(name), includeScopes? _scopeProvider: null, _messageQueue)
            {
                DisableColors = disableColors
            };
        }

        private Func<string, LogLevel, bool> GetFilter(string name)
        {
            if (_filter != null)
            {
                return _filter;
            }

            return falseFilter;
        }

        public void Dispose()
        {
            _messageQueue.Dispose();
        }

    }
}