using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console.Internal;

namespace dexih.remote
{
    [ProviderAlias("Remote")]
    public class ConsoleLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, ConsoleLogger> _loggers = new ConcurrentDictionary<string, ConsoleLogger>();

        private readonly Func<string, LogLevel, bool> _filter;
        
        private readonly ConsoleLoggerProcessor _messageQueue = new ConsoleLoggerProcessor();

        private static readonly Func<string, LogLevel, bool> trueFilter = (cat, level) => true;
        private static readonly Func<string, LogLevel, bool> falseFilter = (cat, level) => false;
        private readonly bool _includeScopes;
        private readonly bool _disableColors;
        private IExternalScopeProvider _scopeProvider;

        public ConsoleLoggerProvider(LogLevel minLevel)
            : this((category, logLevel) => logLevel >= minLevel, false, false)
        {
        }

        public ConsoleLoggerProvider(Func<string, LogLevel, bool> filter, bool includeScopes, bool disableColors)
        {
            _filter = filter ?? throw new ArgumentNullException(nameof(filter));
            _includeScopes = includeScopes;
            _disableColors = disableColors;
        }

        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, CreateLoggerImplementation);
        }

        private ConsoleLogger CreateLoggerImplementation(string name)
        {
            var includeScopes = _includeScopes;
            var disableColors = _disableColors;
            
            return new ConsoleLogger(name, GetFilter(name), includeScopes? _scopeProvider: null, _messageQueue)
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