using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace dexih.remote
{
    [ProviderAlias("Remote")]
    public class FileLoggerProvider: ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, FileLogger> _loggers = new ConcurrentDictionary<string, FileLogger>();

        private readonly Func<string, LogLevel, bool> _filter;
        
        private readonly FileLoggerProcessor _messageQueue;

        private static readonly Func<string, LogLevel, bool> trueFilter = (cat, level) => true;
        private static readonly Func<string, LogLevel, bool> falseFilter = (cat, level) => false;
        private readonly bool _includeScopes;
        private IExternalScopeProvider _scopeProvider;

        public FileLoggerProvider(LogLevel minLevel, string path, string fileName = null)
            : this((category, logLevel) => logLevel >= minLevel, false, path, fileName)
        {
        }

        public FileLoggerProvider(Func<string, LogLevel, bool> filter, bool includeScopes, string path, string fileName = null)
        {
            _filter = filter ?? throw new ArgumentNullException(nameof(filter));
            _includeScopes = includeScopes;
            
            _messageQueue = new FileLoggerProcessor(path, fileName);
        }

        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, CreateLoggerImplementation);
        }

        private FileLogger CreateLoggerImplementation(string name)
        {
            var includeScopes = _includeScopes;
            
            return new FileLogger(name, GetFilter(name), includeScopes? _scopeProvider: null, _messageQueue);
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