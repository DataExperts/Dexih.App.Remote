//  using System;
//  using Microsoft.Extensions.DependencyInjection;
//  using Microsoft.Extensions.DependencyInjection.Extensions;
//  using Microsoft.Extensions.Logging;
//  using Microsoft.Extensions.Logging.Configuration;
//  using Microsoft.Extensions.Logging.Console;
//  using Microsoft.Extensions.Options;
//
//  namespace dexih.remote
//  {
//
//  /// <summary>
//  /// Extension methods for the <see cref="T:Microsoft.Extensions.Logging.ILoggerFactory" /> class.
//  /// </summary>
//  public static class DexihLoggerFactoryExtensions
//  {
//   
//    public static ILoggingBuilder AddDexihConsole(this ILoggingBuilder builder)
//    {
//      builder.AddConfiguration();
//      builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, DexihConsoleLoggerProvider>());
//      builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IConfigureOptions<ConsoleLoggerOptions>, ConsoleLoggerOptionsSetup>());
//      builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IOptionsChangeTokenSource<ConsoleLoggerOptions>, LoggerProviderOptionsChangeTokenSource<ConsoleLoggerOptions, DexihConsoleLoggerProvider>>());
//      return builder;    
//    }
//
//    public static ILoggingBuilder AddDexihConsole(this ILoggingBuilder builder, Action<ConsoleLoggerOptions> configure)
//    {
//      builder.AddDexihConsole();
//      builder.Services.Configure(configure);
//
//      return builder;
//    }
//
//    private class ConsoleLoggerOptionsSetup : ConfigureFromConfigurationOptions<ConsoleLoggerOptions>
//    {
//      public ConsoleLoggerOptionsSetup(
//        ILoggerProviderConfiguration<DexihConsoleLoggerProvider> providerConfiguration)
//        : base(providerConfiguration.Configuration)
//      {
//      }
//    }
//  }
//}
