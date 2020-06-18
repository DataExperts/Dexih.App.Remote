using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations.Alerts;
using dexih.remote.config;
using dexih.remote.operations;
using dexih.remote.operations.Logger;
using dexih.repository;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote
{
    public class Program
    {
        private static readonly Dictionary<string, string> commandlineMappings = new Dictionary<string, string>()
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

        public static int Main(string[] args)
        {
            // mutex used to ensure the remote agent is only running once.
            var mutex = new Mutex(true, "dexih.remote", out var createdNew);
    
            if (!createdNew)
            {
                Console.WriteLine("Only one instance of the remote agent is allowed.");
                return (int) EExitCode.Terminated;
            }
            
            var returnValue = StartAsync(args).Result;
            
            mutex.ReleaseMutex();
            
            return returnValue;
        }
        
        public static async Task<int> StartAsync(string[] args)
        {
            Welcome();
            WriteVersion();
            
            // create a temporary logger (until the log level settings have been loaded).
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var logger = loggerFactory.CreateLogger("main");

            var configDirectory = Environment.GetEnvironmentVariable("DEXIH_CONFIG_DIRECTORY");
            if (string.IsNullOrEmpty(configDirectory))
            {
                configDirectory = Directory.GetCurrentDirectory();
            }
            
            var currentDomain = AppDomain.CurrentDomain;
            currentDomain.AssemblyResolve += LoadAssembly;
            
            var settingsFile = Path.Combine(configDirectory, "appsettings.json");
            if (args.Length >= 2 && args[0] == "-appsettings")
            {
                settingsFile = args[1];
            }
            
            //check config file first for any settings.
            if (File.Exists(settingsFile))
            {
                logger.LogInformation($"Reading settings from the file {settingsFile}.");
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

            var programExit = new ProgramExit();
            
            var hostBuilder = new HostBuilder()
                .ConfigureLogging((hostContext, configLogging) =>
                {
                    configLogging.AddConfiguration(hostContext.Configuration.GetSection("Logging"));
                    configLogging.AddConsole();
                    configLogging.AddDebug();
                })
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

                    var remoteSettings = configApp.Build().Get<RemoteSettings
                    >() ?? new RemoteSettings();
                    
                    remoteSettings.NamingStandards.LoadDefault();
                    var namingStandards = remoteSettings.NamingStandards.ToDictionary(c => "NamingStandards:" + c.Key,c => c.Value);
                    configApp.AddInMemoryCollection(namingStandards);
                    configApp.AddUserInput();
                    configApp.AddInMemoryCollection(otherSettings);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHttpClient();
                    services.AddMemoryCache();
                    services.AddSingleton<ISharedSettings, SharedSettings>();
                    services.AddSingleton(programExit);
                    services.AddHostedService<UpgradeService>();
                    services.AddSingleton<IManagedTasks, ManagedTasksService>();
                    services.AddSingleton<IMessageQueue, MessageQueue>();
                    services.AddSingleton<IAlertQueue, AlertQueue>();
                    services.AddSingleton<ILiveApis, LiveApis>();
                    services.AddSingleton<IRemoteOperations, RemoteOperations>();
                    services.AddHostedService<MessageService>();
                    services.AddHostedService<AlertService>();
                    services.AddHostedService<HttpService>();
                    services.AddHostedService<ListenerService>();
                    services.AddHostedService<AutoStartService>();
                })
                .ConfigureLogging((hostContext, configLogging) =>
                {
                    var alerts = hostContext.Configuration.GetSection("Alerts");
                    if (alerts != null && alerts.GetValue<bool>("AlertOnCritical"))
                    {
                        configLogging.AddAlert();
                    }
                })
                .UseConsoleLifetime();

            var host = hostBuilder.Build();

            var sharedSettings = host.Services.GetService<ISharedSettings>();
            var clientFactory = host.Services.GetService<IHttpClientFactory>();

            var upgrade = await sharedSettings.RemoteSettings.CheckUpgrade(logger, clientFactory);

            if(upgrade && sharedSettings.RemoteSettings.AppSettings.AutoUpgrade)
            {
                await host.StopAsync();
                host.Dispose();
                return (int)EExitCode.Upgrade;
            }

            await sharedSettings.RemoteSettings.GetPlugins(logger, clientFactory);

            try
            {
                await host.RunAsync();
            }
            catch (OperationCanceledException)
            {
                
            }
            
            host.Dispose();
            
            if (programExit.CompleteUpgrade)
            {
                return (int)EExitCode.Upgrade;
            }
            return (int)EExitCode.Success;;
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

Welcome to Dexih - The Data Experts Integration Hub
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

        private static readonly string[] AssemblyPaths = new[]
        {
            Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
            Path.Combine(Directory.GetCurrentDirectory(), "plugins", "standard"),
            Path.Combine(Directory.GetCurrentDirectory(), "plugins", "connections"),
            Path.Combine(Directory.GetCurrentDirectory(), "plugins", "functions")
        };
        
        private static Assembly LoadAssembly(object sender, ResolveEventArgs args)
        {
            foreach (var path in AssemblyPaths)
            {
                var assemblyPath = Path.Combine(path, new AssemblyName(args.Name).Name + ".dll");
                if (File.Exists(assemblyPath))
                {
                    var assembly = Assembly.LoadFrom(assemblyPath);
                    return assembly;
                }
            }

            if(File.Exists(args.RequestingAssembly.Location))
            {
                var assembly = Assembly.LoadFrom(args.RequestingAssembly.Location);
                return assembly;
            }
            
            throw new DllNotFoundException($"The assembly {args.Name} was not found.");
        }
    }
}
