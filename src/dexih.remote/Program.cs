using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Threading.Tasks;
using dexih.operations;
using dexih.repository;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore.ValueGeneration.Internal;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace dexih.remote
{
    public class Program
    {
        
        public static async Task<int> Main(string[] args)
        {
            Welcome();
            
            // create a temporary logger (until the log level settings have been loaded.
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new RemoteLoggerProvider(LogLevel.Trace));
            var logger = loggerFactory.CreateLogger("main");


            var settingsFile = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json");
            if (args.Length >= 2 && args[0] == "-appsettings")
            {
                settingsFile = args[1];
            }
            
            // Set up configuration sources.
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory());

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
            
            // add usersecrets when development mode
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            if (environment == "Development")
            {
                builder.AddUserSecrets<Program>();
            }
            var configuration = builder.Build();
            var remoteSettings = configuration.Get<RemoteSettings>();

            // call configure settings, to get additional settings
            var configureSettings = new ConfigureSettings(logger, remoteSettings);

            // add any values from the command line.
            if (!configureSettings.AddCommandLineValues(args))
            {
                return (int) DexihRemote.EExitCode.InvalidSetting;
            }

            // if an upgrade is required return the ExitCode.Upgrade value, which will be picked up by executing script to complete upgrade.
            if (configureSettings.CheckUpgrade().Result)
            {
                return (int) DexihRemote.EExitCode.Upgrade;
            }

            // get user input for any settings which are not complete.
            configureSettings.GetUserInput();
            
            // dispose the old logger, and create a new one now the log level is known.
            loggerFactory.Dispose();
            
            // add logging.
            loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new RemoteLoggerProvider(configureSettings.RemoteSettings.Logging.LogLevel.Default));

            var remote = new DexihRemote(configureSettings.RemoteSettings, loggerFactory);
            var exitCode = await remote.StartAsync(configureSettings.SaveSettings, CancellationToken.None);

            return (int) exitCode;
        }
        
        public static void Welcome()
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

    }
}
