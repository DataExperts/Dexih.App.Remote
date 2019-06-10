using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Logging.Console.Internal;
using Microsoft.Extensions.Options;

namespace dexih.remote
{
    [ProviderAlias("Remote")]
    public class DexihConsoleLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, DexihConsoleLogger> _loggers = new ConcurrentDictionary<string, DexihConsoleLogger>();

        private readonly Func<string, LogLevel, bool> _filter;
        
        private readonly ConsoleLoggerProcessor _messageQueue = new ConsoleLoggerProcessor();

        private static readonly Func<string, LogLevel, bool> trueFilter = (cat, level) => true;
        private static readonly Func<string, LogLevel, bool> falseFilter = (cat, level) => false;
        private readonly bool _includeScopes;
        private readonly bool _disableColors;
        private IExternalScopeProvider _scopeProvider;

        public DexihConsoleLoggerProvider()
        {
            _filter = trueFilter;
            _includeScopes = false;
            _disableColors = false;
        }

        public DexihConsoleLoggerProvider(IOptions<ConsoleLoggerOptions> options)
        {
            _filter = (category, logLevel) => logLevel >= LogLevel.Information;
            _includeScopes = options.Value.IncludeScopes;
            _disableColors = options.Value.DisableColors;
        }

        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, CreateLoggerImplementation);
        }

        private DexihConsoleLogger CreateLoggerImplementation(string name)
        {
            var includeScopes = _includeScopes;
            var disableColors = _disableColors;
            
            return new DexihConsoleLogger(name, GetFilter(name), includeScopes? _scopeProvider: null, _messageQueue)
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