using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.remote.config;
using dexih.remote.operations;
using dexih.repository;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace dexih.remote
{
    public class Program
    {
        public static Dictionary<string, string> commandlineMappings = new Dictionary<string, string>()
        {
            {"-u", "AppSettings:User"},
            {"--user", "AppSettings:User"},
            {"--username", "AppSettings:User"},
            {"-t", "AppSettings:UserToken"},
            {"--token", "AppSettings:UserToken"},
            {"--usertoken", "AppSettings:UserToken"},
            {"-w", "AppSettings:WebServer"},
            {"--webserver", "AppSettings:WebServer"},
            {"-p", "Runtime:Password"},
            {"--password", "Runtime:Password"},
            {"-k", "AppSettings:EncryptionKey"},
            {"-key", "AppSettings:EncryptionKey"},
            {"-i", "AppSettings:RemoteAgentId"},
            {"--id", "AppSettings:RemoteAgentId"},
            {"-n", "AppSettings:Name"},
            {"--name", "AppSettings:Name"},
            {"-l", "Logging:LogLevel:Default"},
            {"--log", "Logging:LogLevel:Default"},
            {"--loglevel", "Logging:LogLevel:Default"},
            {"-s", "Runtime:SaveSettings"},
            {"--save", "Runtime:SaveSettings"},
            {"--prompt", "AppSettings:UserPrompt"},
            {"--userprompt", "AppSettings:UserPrompt"},
            {"--up", "AppSettings:AutoUpgrade"},
            {"--upgrade", "AppSettings:AutoUpgrade"},
            {"--pre", "AppSettings:AllowPreReleases"},
            {"--prerelease", "AppSettings:AllowPreReleases"},
            {"--https", "Network:EnforceHttps"},
            {"--allowlan", "Privacy:AllowLandAccess"},
            {"--allowexternal", "Privacy:AllowExternalAccess"},
            {"--allowproxy", "Privacy:AllowProxy"},
            {"--certificatefilename", "Network:CertificateFilename"},
            {"--certificatepassword", "Network:CertificatePassword"},
            {"--autogeneratecertificate", "Network:AutoGenerateCertificate"},
            {"--enableupnp", "Network:EnableUPnP"},
        };

        public static async Task<int> Main(string[] args)
        {
            var mutex = new Mutex(true, "dexih.remote", out var createdNew);

            if (!createdNew)
            {
                Console.WriteLine("Only one instance of the remote agent is allowed.");
                return (int) EExitCode.Terminated;
            }
            
            var returnValue = await StartAsync(args);
            
            return returnValue;
        }
        
        public static async Task<int> StartAsync(string[] args)
        {
            Welcome();
            WriteVersion();
            
            // create a temporary logger (until the log level settings have been loaded.
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new ConsoleLoggerProvider((s, level) => true, true ));
            var logger = loggerFactory.CreateLogger("main");

            var configDirectory = Environment.GetEnvironmentVariable("DEXIH_CONFIG_DIRECTORY");
            if (string.IsNullOrEmpty(configDirectory))
            {
                configDirectory = Directory.GetCurrentDirectory();
            }
            
            var settingsFile = Path.Combine(configDirectory, "appsettings.json");
            if (args.Length >= 2 && args[0] == "-appsettings")
            {
                settingsFile = args[1];
            }
            
            //check config file first for any settings.
            if (File.Exists(settingsFile))
            {
                logger.LogInformation($"Reading settings from the file {settingsFile}.");
//                builder.AddJsonFile(settingsFile);
            }
            else
            {
                logger.LogInformation($"Could not find the settings file {settingsFile}.");
            }

            var otherSettings = new Dictionary<string, string>()
            {
                {"Runtime:ConfigDirectory", configDirectory},
                {"Runtime:AppSettingsPath", settingsFile},
                {"Runtime:LocalIpAddress", LocalIpAddress(logger)},
                {"Runtime:Version", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion},
            };


            var hostBuilder = new HostBuilder()
                .ConfigureHostConfiguration(configHost =>
                {
                    configHost.SetBasePath(configDirectory);
                    configHost.AddJsonFile(settingsFile, optional: true);
                    configHost.AddEnvironmentVariables(prefix: "DEXIH_");
                })
                .ConfigureAppConfiguration((hostContext, configApp) =>
                {
                    // add user secrets when development mode
                    if (hostContext.HostingEnvironment.IsDevelopment())
                    {
                        configApp.AddUserSecrets<Program>();
                    }

                    configApp.AddCommandLine(args, commandlineMappings);

                    var remoteSettings = configApp.Build().Get<RemoteSettings>() ?? new RemoteSettings();
                    remoteSettings.NamingStandards.LoadDefault();
                    
                    if (remoteSettings.AppSettings.AutoUpgrade && remoteSettings.CheckUpgrade().Result)
                    {
                        otherSettings.Add("Runtime:DoUpgrade", "true");
                    }
                    else
                    {
                        configApp.AddUserInput();
                    }
                    
                    configApp.AddInMemoryCollection(otherSettings);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddMemoryCache();
                    services.AddSingleton<ISharedSettings, SharedSettings>();
                    services.AddHostedService<UpgradeService>();
                    services.AddSingleton<IManagedTasks, ManagedTasks>();

                    // don't load there other services if an upgrade is pending.
                    var doUpgrade = hostContext.Configuration.GetValue<bool>("Runtime:DoUpgrade");
                    if (doUpgrade)
                    {
                        return;
                    }

                    services.AddSingleton<IMessageQueue, MessageQueue>();
                    services.AddSingleton<ILiveApis, LiveApis>();
                    services.AddSingleton<IRemoteOperations, RemoteOperations>();
                    services.AddHostedService<MessageService>();
                    services.AddHostedService<HttpService>();
                    services.AddHostedService<ListenerService>();
                    services.AddHostedService<AutoStartService>();
                })
                .ConfigureLogging((hostContext, configLogging) =>
                {
                    configLogging.AddConfiguration(hostContext.Configuration.GetSection("Logging"));
                    //configLogging.AddDexihConsole();
                    configLogging.AddConsole();
                    configLogging.AddDebug();
                })
                .UseConsoleLifetime();

            var host = hostBuilder.Build();

            var sharedSettings = host.Services.GetService<ISharedSettings>();

            try
            {
                await host.RunAsync();
            }
            catch (OperationCanceledException)
            {
                
            }
            
            if (sharedSettings.CompleteUpgrade)
            {
//                var process = Process.GetCurrentProcess().Id;
//                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
//                {
//                    if (File.Exists("dexih.remote.upgrade.exe"))
//                    {
//                        Process.Start("dexih.remote.upgrade.exe", process.ToString());
//                    }
//                }
//                else
//                {
//                    if (File.Exists("dexih.remote.upgrade"))
//                    {
//                        Process.Start("dexih.remote.upgrade", process.ToString());
//                    }
//                }
                
                return (int)EExitCode.Upgrade;
            }
            
//            // Set up configuration sources.
//            var builder = new ConfigurationBuilder().SetBasePath(configDirectory);
//
//            //check config file first for any settings.
//            if (File.Exists(settingsFile))
//            {
//                logger.LogInformation($"Reading setting from the {settingsFile}.");
//                builder.AddJsonFile(settingsFile);
//            }
//            else
//            {
//                logger.LogInformation($"Could not find the settings file {settingsFile}.");
//            }
//
//            // add environment variables second.
//            builder.AddEnvironmentVariables();
//            
//            // add user secrets when development mode
//            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
//            if (environment == "Development")
//            {
//                builder.AddUserSecrets<Program>();
//            }
//            var configuration = builder.Build();
//            var remoteSettings = configuration.Get<RemoteSettings>();
//            remoteSettings.Runtime.ConfigDirectory = configDirectory;
//            remoteSettings.Runtime.AppSettingsPath = settingsFile;
//
//            // call configure settings, to get additional settings
//            var configureSettings = new ConfigureSettings(logger, remoteSettings);
//
//            // add any values from the command line.
//            if (!configureSettings.AddCommandLineValues(args))
//            {
//                return (int) DexihRemote.EExitCode.InvalidSetting;
//            }
//
//            // if an upgrade is required return the ExitCode.Upgrade value, which will be picked up by executing script to complete upgrade.
//            try
//            {
//                var update = remoteSettings.CheckUpgrade().Result;
//                
//                if (update)
//                {
//                    File.WriteAllText("latest_version.txt", remoteSettings.Runtime.LatestVersion + "\n" + remoteSettings.Runtime.LatestDownloadUrl);
//                    logger?.LogWarning($"The local version of the remote agent is v{remoteSettings.Runtime.Version}.");
//                    logger?.LogWarning($"The latest version of the remote agent is {remoteSettings.Runtime.LatestVersion}.");
//                    logger?.LogWarning($"There is a newer release of the remote agent available at {remoteSettings.Runtime.LatestDownloadUrl}.");
//
//                    if (remoteSettings.AppSettings.AutoUpgrade)
//                    {
//                        logger?.LogWarning(
//                            "The application will exit so an upgrade can be completed.  To skip upgrade checks include \"-skipupgrade\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");
//
//                        return (int) DexihRemote.EExitCode.Upgrade;
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                logger.LogError(ex,$"There was an error checking for update.  Message: {ex.Message}");
//            }
//            
//            // get user input for any settings which are not complete.
//            configureSettings.GetUserInput();
//            
//            configureSettings.RemoteSettings.NamingStandards.LoadDefault();
//            
//            // dispose the old logger, and create a new one now the log level is known.
//            loggerFactory.Dispose();
//            
//            // add logging.
//            loggerFactory = new LoggerFactory();
//            loggerFactory.AddProvider(new ConsoleLoggerProvider(configureSettings.RemoteSettings.Logging.LogLevel.Default));
//            if (!string.IsNullOrEmpty(remoteSettings.Logging.LogFilePath))
//            {
//                loggerFactory.AddProvider(
//                    new FileLoggerProvider(configureSettings.RemoteSettings.Logging.LogLevel.Default, remoteSettings.Logging.LogFilePath));
//            }
//
//            var remote = new DexihRemote(configureSettings.RemoteSettings, loggerFactory);
//            var exitCode = await remote.StartAsync(configureSettings.SaveSettings, CancellationToken.None);
//
//            return (int) exitCode;

            return 0;
        }

        private static void Welcome()
        {
            Console.WriteLine(@"
 _______   _______ ___   ___  __   __    __  
|       \ |   ____|\  \ /  / |  | |  |  |  | 
|  .--.  ||  |__    \  V  /  |  | |  |__|  | 
|  |  |  ||   __|    >   <   |  | |   __   | 
|  '--'  ||  |____  /  .  \  |  | |  |  |  | 
|_______/ |_______|/__/ \__\ |__| |__|  |__| 

Welcome to Dexih - The Data Experts Information Hub
");
            
            // introduction message, with file version
            var runtimeVersion = Assembly.GetEntryAssembly()
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

            Console.WriteLine($"Remote Agent - Version {runtimeVersion}");
            
        }

        private static void WriteVersion()
        {
            var assembly = Assembly.GetEntryAssembly();
            var localVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            File.WriteAllText(assembly.GetName().Name + ".version", localVersion);
        }
        
        /// <summary>
        /// Gets the local ip address
        /// </summary>
        /// <returns></returns>
        private static string LocalIpAddress(ILogger logger)
        {
            try
            {
                using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0))
                {
                    socket.Connect("8.8.8.8", 65530);
                    if (!(socket.LocalEndPoint is IPEndPoint endPoint))
                    {
                        logger.LogError(
                            "The local network ip address could not be determined automatically.  Defaulting to local IP (127.0.0.1)");
                        return "127.0.0.1";
                    }

                    return endPoint.Address.ToString();
                }
            }
            catch (Exception ex)
            {
                logger.LogError(
                    "The local network ip address could not be determined automatically.  Defaulting to local IP (127.0.0.1)",
                    ex);
                return "127.0.0.1";
            }
        }
        
    }
}
