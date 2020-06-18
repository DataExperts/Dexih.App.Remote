using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations.Logger
{
    public static class AlertLoggerFactoryExtensions
    {
        /// <summary>
        /// Adds a debug logger named 'Debug' to the factory.
        /// </summary>
        /// <param name="builder">The extension method argument.</param>
        public static ILoggingBuilder AddAlert(this ILoggingBuilder builder)
        {
            builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, AlertLoggerProvider>());
            return builder;
        }
    }
}