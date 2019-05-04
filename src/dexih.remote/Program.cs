using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.repository;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace dexih.remote
{
    public class Program
    {
        
        public static async Task<int> Main(string[] args)
        {
            Welcome();
            
            // create a temporary logger (until the log level settings have been loaded.
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new ConsoleLoggerProvider(LogLevel.Trace));
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
            
            // Set up configuration sources.
            var builder = new ConfigurationBuilder().SetBasePath(configDirectory);

            //check config file first for any settings.
            if (File.Exists(settingsFile))
            {
                logger.LogInformation($"Reading setting from the {settingsFile}.");
                builder.AddJsonFile(settingsFile);
            }
            else
            {
                logger.LogInformation($"Could not find the settings file {settingsFile}.");
            }

            // add environment variables second.
            builder.AddEnvironmentVariables();
            
            // add user secrets when development mode
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            if (environment == "Development")
            {
                builder.AddUserSecrets<Program>();
            }
            var configuration = builder.Build();
            var remoteSettings = configuration.Get<RemoteSettings>();
            remoteSettings.Runtime.ConfigDirectory = configDirectory;
            remoteSettings.Runtime.AppSettingsPath = settingsFile;

            // call configure settings, to get additional settings
            var configureSettings = new ConfigureSettings(logger, remoteSettings);

            // add any values from the command line.
            if (!configureSettings.AddCommandLineValues(args))
            {
                return (int) DexihRemote.EExitCode.InvalidSetting;
            }

            // if an upgrade is required return the ExitCode.Upgrade value, which will be picked up by executing script to complete upgrade.
            try
            {
                var update = remoteSettings.CheckUpgrade().Result;
                
                if (update)
                {
                    File.WriteAllText("latest_version.txt", remoteSettings.Runtime.LatestVersion + "\n" + remoteSettings.Runtime.LatestDownloadUrl);
                    logger?.LogWarning($"The local version of the remote agent is v{remoteSettings.Runtime.Version}.");
                    logger?.LogWarning($"The latest version of the remote agent is {remoteSettings.Runtime.LatestVersion}.");
                    logger?.LogWarning($"There is a newer release of the remote agent available at {remoteSettings.Runtime.LatestDownloadUrl}.");

                    if (remoteSettings.AppSettings.AutoUpgrade)
                    {
                        logger?.LogWarning(
                            "The application will exit so an upgrade can be completed.  To skip upgrade checks include \"-skipupgrade\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");

                        return (int) DexihRemote.EExitCode.Upgrade;
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex,$"There was an error checking for update.  Message: {ex.Message}");
            }
            
            

            // get user input for any settings which are not complete.
            configureSettings.GetUserInput();
            
            configureSettings.RemoteSettings.NamingStandards.LoadDefault();
            
            // dispose the old logger, and create a new one now the log level is known.
            loggerFactory.Dispose();
            
            // add logging.
            loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new ConsoleLoggerProvider(configureSettings.RemoteSettings.Logging.LogLevel.Default));
            if (!string.IsNullOrEmpty(remoteSettings.Logging.LogFilePath))
            {
                loggerFactory.AddProvider(
                    new FileLoggerProvider(configureSettings.RemoteSettings.Logging.LogLevel.Default, remoteSettings.Logging.LogFilePath));
            }

            var remote = new DexihRemote(configureSettings.RemoteSettings, loggerFactory);
            var exitCode = await remote.StartAsync(configureSettings.SaveSettings, CancellationToken.None);

            return (int) exitCode;
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
    }
}
