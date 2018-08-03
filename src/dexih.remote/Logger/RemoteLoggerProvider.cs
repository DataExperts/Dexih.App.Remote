﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Logging.Console.Internal;
using Microsoft.Extensions.Options;

namespace dexih.remote
{
    [ProviderAlias("Remote")]
    public class RemoteLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, RemoteLogger> _loggers = new ConcurrentDictionary<string, RemoteLogger>();

        private readonly Func<string, LogLevel, bool> _filter;
        private IConsoleLoggerSettings _settings;
        private readonly ConsoleLoggerProcessor _messageQueue = new ConsoleLoggerProcessor();

        private static readonly Func<string, LogLevel, bool> trueFilter = (cat, level) => true;
        private static readonly Func<string, LogLevel, bool> falseFilter = (cat, level) => false;
        private IDisposable _optionsReloadToken;
        private bool _includeScopes;
        private bool _disableColors;
        private IExternalScopeProvider _scopeProvider;


        public RemoteLoggerProvider(LogLevel minLevel)
            : this((category, logLevel) => logLevel >= minLevel, false, false)
        {
        }

        public RemoteLoggerProvider(Func<string, LogLevel, bool> filter, bool includeScopes)
            : this(filter, includeScopes, false)
        {
        }

        public RemoteLoggerProvider(Func<string, LogLevel, bool> filter, bool includeScopes, bool disableColors)
        {
            if (filter == null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            _filter = filter;
            _includeScopes = includeScopes;
            _disableColors = disableColors;
        }

        public RemoteLoggerProvider(IOptionsMonitor<ConsoleLoggerOptions> options)
        {
            // Filter would be applied on LoggerFactory level
            _filter = trueFilter;
            _optionsReloadToken = options.OnChange(ReloadLoggerOptions);
            ReloadLoggerOptions(options.CurrentValue);
        }

        private void ReloadLoggerOptions(ConsoleLoggerOptions options)
        {
            _includeScopes = options.IncludeScopes;
            _disableColors = options.DisableColors;
            var scopeProvider = GetScopeProvider();
            foreach (var logger in _loggers.Values)
            {
                logger.ScopeProvider = scopeProvider;
                logger.DisableColors = options.DisableColors;
            }
        }

        public RemoteLoggerProvider(IConsoleLoggerSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            _settings = settings;

            if (_settings.ChangeToken != null)
            {
                _settings.ChangeToken.RegisterChangeCallback(OnConfigurationReload, null);
            }
        }

        private void OnConfigurationReload(object state)
        {
            try
            {
                // The settings object needs to change here, because the old one is probably holding on
                // to an old change token.
                _settings = _settings.Reload();

                _includeScopes = _settings?.IncludeScopes ?? false;

                var scopeProvider = GetScopeProvider();
                foreach (var logger in _loggers.Values)
                {
                    logger.Filter = GetFilter(logger.Name, _settings);
                    logger.ScopeProvider = scopeProvider;
                }
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error while loading configuration changes.{Environment.NewLine}{ex}");
            }
            finally
            {
                // The token will change each time it reloads, so we need to register again.
                if (_settings?.ChangeToken != null)
                {
                    _settings.ChangeToken.RegisterChangeCallback(OnConfigurationReload, null);
                }
            }
        }

        public ILogger CreateLogger(string name)
        {
            return _loggers.GetOrAdd(name, CreateLoggerImplementation);
        }

        private RemoteLogger CreateLoggerImplementation(string name)
        {
            var includeScopes = _settings?.IncludeScopes ?? _includeScopes;
            var disableColors = _disableColors;

            return new RemoteLogger(name, GetFilter(name, _settings), includeScopes? _scopeProvider: null, _messageQueue)
                {
                    DisableColors = disableColors
                };
        }

        private Func<string, LogLevel, bool> GetFilter(string name, IConsoleLoggerSettings settings)
        {
            if (_filter != null)
            {
                return _filter;
            }

            if (settings != null)
            {
                foreach (var prefix in GetKeyPrefixes(name))
                {
                    if (settings.TryGetSwitch(prefix, out var level))
                    {
                        return (n, l) => l >= level;
                    }
                }
            }

            return falseFilter;
        }

        private IEnumerable<string> GetKeyPrefixes(string name)
        {
            while (!string.IsNullOrEmpty(name))
            {
                yield return name;
                var lastIndexOfDot = name.LastIndexOf('.');
                if (lastIndexOfDot == -1)
                {
                    yield return "Default";
                    break;
                }
                name = name.Substring(0, lastIndexOfDot);
            }
        }

        private IExternalScopeProvider GetScopeProvider()
        {
            if (_includeScopes && _scopeProvider == null)
            {
                _scopeProvider = new LoggerExternalScopeProvider();
            }
            return _includeScopes ? _scopeProvider : null;
        }

        public void Dispose()
        {
            _optionsReloadToken?.Dispose();
            _messageQueue.Dispose();
        }

        public void SetScopeProvider(IExternalScopeProvider scopeProvider)
        {
            _scopeProvider = scopeProvider;
        }
    }
}