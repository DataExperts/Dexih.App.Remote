using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using dexih.operations;
using dexih.repository;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore.ValueGeneration.Internal;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace dexih.remote
{
    public class Program
    {
        private enum ExitCode {
            Success = 0,
            InvalidSetting = 1,
            InvalidLogin = 2,
            Terminated = 3,
            UnknownError = 10,
            Upgrade = 20
        }
        
        private static DexihRemote Remote { get; set; }
       
        public static int Main(string[] args)
        {
            
            // add logging.
            var loggerFactory = new LoggerFactory();

            // add the logging level output to the console.
            loggerFactory.AddConsole();
            loggerFactory.AddDebug();
            var logger = loggerFactory.CreateLogger("dexih.remote main");

            var remoteSettings = new RemoteSettings
            {
                AppSettings =
                {
                    WebServer = "https://dexih.dataexpertsgroup.com"
                }
            };

            var settingsFile = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json");

            if (args.Length >= 2 && args[0] == "-appsettings")
            {
                settingsFile = args[1];
            }

            // Set up configuration sources.
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory());

            //check config file first for any settings.
            if (File.Exists(settingsFile))
            {
                builder.AddJsonFile(settingsFile);
            }

            // add environment variables second.
            builder.AddEnvironmentVariables();

            // add usersecrets when development
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            if (environment == "Development")
            {
                builder.AddUserSecrets<Program>();
            }

            var configuration = builder.Build();
            remoteSettings = configuration.Get<RemoteSettings>();

            // if the first time running, then prompt user for details.
            var saveSettings = remoteSettings.AppSettings.FirstRun;
            var resetSettings = remoteSettings.AppSettings.FirstRun;

            //check command line for settings.  command line overrides settings file.
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-u":
                    case "-user":
                    case "-username":
                        i++;
                        remoteSettings.AppSettings.User = args[i];
                        break;
                    case "-t":
                    case "-token":
                    case "-usertoken":
                        i++;
                        remoteSettings.AppSettings.UserToken = args[i];
                        break;
                    case "-w":
                    case "-webserver":
                        i++;
                        remoteSettings.AppSettings.WebServer = args[i];
                        break;
                    case "-p":
                    case "-password":
                        i++;
                        remoteSettings.AppSettings.Password = args[i];
                        break;
                    case "-k":
                    case "-key":
                        i++;
                        remoteSettings.AppSettings.EncryptionKey = args[i];
                        break;
                    case "-i":
                    case "-id":
                        i++;
                        remoteSettings.AppSettings.RemoteAgentId = args[i];
                        break;
                    case "-n":
                    case "-name":
                        i++;
                        remoteSettings.AppSettings.Name = args[i];
                        break;
                    case "-l":
                    case "-log":
                    case "-loglevel":
                        i++;
                        var logLevelString = args[i];
                        var checkLogLevel = Enum.TryParse<LogLevel>(logLevelString, out var logLevel);
                        if (!checkLogLevel)
                        {
                            Console.WriteLine("The log level setting was not recognised.  The value was: {0}",
                                logLevelString);
                            return (int)ExitCode.InvalidSetting;
                        }

                        remoteSettings.Logging.LogLevel.Default = logLevel;
                        break;
                    case "-s":
                    case "-save":
                        saveSettings = true;
                        break;
                    case "-r":
                    case "-reset":
                        resetSettings = true;
                        break;
                    case "-up":
                    case "-upgrade":
                        remoteSettings.AppSettings.AutoUpgrade = true;
                        break;
                    case "-skipupgrade":
                        remoteSettings.AppSettings.AutoUpgrade = false;
                        break;
                    case "-pr":
                    case "-prerelease":
                        remoteSettings.AppSettings.AllowPreReleases = true;
                        break;
                }
            }

            // check if there is a newer release.
            if (remoteSettings.AppSettings.AutoUpgrade)
            {
                string downloadUrl = null;
                string latestVersion = null;
                var logger1 = logger;
                Task.Run(async () =>
                {
                    try
                    {
                        using (var httpClient = new HttpClient())
                        {
                            httpClient.DefaultRequestHeaders.Add("User-Agent", "Dexih Remote Agent");
                            JToken jToken;
                            if (remoteSettings.AppSettings.AllowPreReleases)
                            {
                                // this api gets all releases.
                                var response = await httpClient.GetAsync("https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases");
                                var responseText = await response.Content.ReadAsStringAsync();
                                var releases = JArray.Parse(responseText);
                                // the first release will be the latest.
                                jToken = releases[0];
                            }
                            else
                            {
                                // this api gets the latest release, excluding prereleases.
                                var response = await httpClient.GetAsync("https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases/latest");
                                var responseText = await response.Content.ReadAsStringAsync();
                                jToken = JToken.Parse(responseText);
                            }

                            latestVersion = (string) jToken["tag_name"];

                            foreach (var asset in jToken["assets"])
                            {
                                var name = ((string) asset["name"]).ToLower();
                                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && name.Contains("windows"))
                                {
                                    downloadUrl = (string) asset["browser_download_url"];
                                    break;
                                }

                                if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) && name.Contains("osx"))
                                {
                                    downloadUrl = (string) asset["browser_download_url"];
                                    break;
                                }

                                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && name.Contains("linux"))
                                {
                                    downloadUrl = (string) asset["browser_download_url"];
                                    break;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        logger1.LogError(ex,
                            "There was an issue getting the latest release url from github.  Error: " + ex.Message);
                    }
                }).Wait();

                if (string.IsNullOrEmpty(downloadUrl))
                {
                    logger.LogError("There was an issue getting the latest release url from github.");
                }
                else
                {
                    var localVersion = Assembly.GetEntryAssembly()
                        .GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

                    var latestBuild = latestVersion.Split('-').Last();
                    var localBuild = localVersion.Split('-').Last();

                    logger.LogWarning($"The local build is {localBuild}.");
                    logger.LogWarning($"The latest build is {latestBuild}.");
                    
                    if (string.CompareOrdinal(latestBuild, localBuild) > 0)
                    {
                        File.WriteAllText("latest_version.txt", latestVersion + "\n" + downloadUrl);
                        logger.LogWarning($"The local version of the remote agent is v{localVersion}.");
                        logger.LogWarning($"The latest version of the remote agent is {latestVersion}.");
                        logger.LogWarning($"There is a newer release of the remote agent available at {downloadUrl}.");
                        logger.LogWarning($"The application will exit so an upgrade can be completed.  To skip upgrade checks include \"-skipupgrade\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");
                        loggerFactory.Dispose();
                        return (int) ExitCode.Upgrade;
                    }
                }
            }
            
            loggerFactory.Dispose();
            
            // add logging.
            loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole(remoteSettings.Logging.LogLevel.Default);
            loggerFactory.AddDebug();
            logger = loggerFactory.CreateLogger("dexih.remote main");

            
            var checkSaveSettings = false;
            
            //any critical settings not received, prompt user.
            if (resetSettings || string.IsNullOrEmpty(remoteSettings.AppSettings.WebServer))
            {
                checkSaveSettings = true;
                
                Console.Write($"Enter the dexih web server [{remoteSettings.AppSettings.WebServer}]: ");
                var webServer = Console.ReadLine();
                
                if (!string.IsNullOrEmpty(webServer))
                {
                    remoteSettings.AppSettings.WebServer = webServer;
                }
            }

            if (resetSettings || string.IsNullOrEmpty(remoteSettings.AppSettings.RemoteAgentId))
            {
                checkSaveSettings = true;

                Console.WriteLine("Enter the unique remote agent id.");
                Console.Write("This works in conjunction with the UserToken to authenticate [auto-generate]: ");
                remoteSettings.AppSettings.RemoteAgentId = Console.ReadLine();
                
                if (string.IsNullOrEmpty(remoteSettings.AppSettings.RemoteAgentId))
                {
                    remoteSettings.AppSettings.RemoteAgentId = Guid.NewGuid().ToString();
                    Console.WriteLine($"New remote agent id is \"{remoteSettings.AppSettings.RemoteAgentId}\".");
                }
            }
            
            if (resetSettings || string.IsNullOrEmpty(remoteSettings.AppSettings.EncryptionKey))
            {
                checkSaveSettings = true;

                if (string.IsNullOrEmpty(remoteSettings.AppSettings.EncryptionKey))
                {
                    Console.WriteLine($"Enter the encryption key.");
                    Console.Write($"This is used to encrypt/decrypt data marked as secure. [auto-generate]: ");
                    remoteSettings.AppSettings.EncryptionKey = Console.ReadLine();
                    if (string.IsNullOrEmpty(remoteSettings.AppSettings.EncryptionKey))
                    {
                        remoteSettings.AppSettings.EncryptionKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
                        Console.WriteLine($"New encryption key \"{remoteSettings.AppSettings.EncryptionKey}\".");
                    }
                }
                else
                {
                    string key;
                    Console.Write($"Enter the encryption key [blank - use current, \"new\" to generate]: ");
                    key = Console.ReadLine();
                    if (string.IsNullOrEmpty(key) || key.ToLower() == "new")
                    {
                        remoteSettings.AppSettings.EncryptionKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
                        Console.WriteLine($"New encryption key \"{remoteSettings.AppSettings.EncryptionKey}\".");
                    }
                   
                }
            }
            
            //any critical settings not received, prompt user.
            if (resetSettings || string.IsNullOrEmpty(remoteSettings.AppSettings.Name))
            {
                checkSaveSettings = true;

                Console.WriteLine($"Allow files to be uploaded through the agent (true/false): [{remoteSettings.AppSettings.AllowDataUpload}]: ");
                var allowUpload = Console.ReadLine();

                if (!string.IsNullOrEmpty(allowUpload))
                {
                    while (allowUpload != "true" && allowUpload != "false")
                    {
                        Console.Write("Enter true/false: ");
                        allowUpload = Console.ReadLine();
                    }
                    remoteSettings.AppSettings.AllowDataUpload = Convert.ToBoolean(allowUpload);
                }

                Console.WriteLine($"Allow files to be downloaded through the agent (true/false): [{remoteSettings.AppSettings.AllowDataDownload}]: ");
                var allowDownload = Console.ReadLine();

                if (!string.IsNullOrEmpty(allowDownload))
                {
                    while (allowDownload != "true" && allowDownload != "false")
                    {
                        Console.Write("Enter true/false: ");
                        allowDownload = Console.ReadLine();
                    }
                    remoteSettings.AppSettings.AllowDataDownload = Convert.ToBoolean(allowDownload);
                }
            }
            
            if (resetSettings || string.IsNullOrEmpty(remoteSettings.AppSettings.User))
            {
                checkSaveSettings = true;

                Console.Write($"Enter the login email [{remoteSettings.AppSettings.User}]: ");
                var user = Console.ReadLine();
                if (!string.IsNullOrEmpty(user))
                {
                    remoteSettings.AppSettings.User = user;
                }
            }

            if (resetSettings || (string.IsNullOrEmpty(remoteSettings.AppSettings.UserToken) && string.IsNullOrEmpty(remoteSettings.AppSettings.Password)))
            {
                checkSaveSettings = true;

                Console.Write("Enter the password [leave empty to specify user token]: ");
                ConsoleKeyInfo key;
                var pass = "";
                do
                {
                    key = Console.ReadKey(true);
                    if (key.Key != ConsoleKey.Backspace && key.Key != ConsoleKey.Enter)
                    {
                        pass += key.KeyChar;
                        Console.Write("*");
                    }
                    else
                    {
                        if (key.Key == ConsoleKey.Backspace && pass.Length > 0)
                        {
                            pass = pass.Substring(0, (pass.Length - 1));
                            Console.Write("\b \b");
                        }
                    }
                } while (key.Key != ConsoleKey.Enter);
                Console.WriteLine();
                remoteSettings.AppSettings.Password = pass;
                
                if (string.IsNullOrEmpty(remoteSettings.AppSettings.Password))
                {                
                    Console.Write("Enter the user token: ");
                    remoteSettings.AppSettings.UserToken = Console.ReadLine();

                    while(string.IsNullOrEmpty(remoteSettings.AppSettings.UserToken))
                    {                
                        Console.Write("No user token or password.");
                    }
                }
                else
                {
                    // if there is a password, and reset settings has been asked, then remove the user token.
                    if (resetSettings)
                    {
                        remoteSettings.AppSettings.UserToken = null;
                    }
                }
            }

            if (resetSettings || string.IsNullOrEmpty(remoteSettings.AppSettings.Name))
            {
                checkSaveSettings = true;

                Console.Write("Enter a name to describe this remote agent [blank use machine name]: ");
                remoteSettings.AppSettings.Name = Console.ReadLine();
                
                if (string.IsNullOrEmpty(remoteSettings.AppSettings.Name))
                {
                    remoteSettings.AppSettings.Name = Environment.MachineName;
                }
            }
            
            if (resetSettings || (checkSaveSettings && !saveSettings))
            {
                Console.Write("Would you like to save settings (enter yes or no) [no]?: ");
                var saveResult = Console.ReadLine().ToLower();

                while(!(saveResult == "yes" || saveResult == "no" || string.IsNullOrEmpty(saveResult)))
                {                
                    Console.Write("Would you like to save settings (enter yes or no) [no]?: ");
                    saveResult = Console.ReadLine().ToLower();
                }

                if (saveResult == "no" || saveResult == "no" || string.IsNullOrEmpty(saveResult))
                {
                    saveSettings = false;
                }
                else if(saveResult == "yes")
                {
                    saveSettings = true;
                }
            }

            Remote = new DexihRemote(remoteSettings, loggerFactory);

            logger.LogInformation("Connecting to server.  ctrl-c to terminate.");

            //use this flag so the retrying only displays once.
            var retryStarted = false;
            var savedSettings = false;

            while (true)
            {
                var generateToken = !string.IsNullOrEmpty(remoteSettings.AppSettings.Password) && saveSettings;
                var loginResult = Remote.LoginAsync(generateToken, retryStarted).Result;

                var connectResult = loginResult.connectionResult;

                if (connectResult == DexihRemote.EConnectionResult.Connected)
                {
                    remoteSettings.AppSettings.IpAddress = loginResult.ipAddress;

                    if (!savedSettings && saveSettings)
                    {
                        remoteSettings.AppSettings.UserToken = loginResult.userToken;
                        
                        var appSettingsFile = Directory.GetCurrentDirectory() + "/appsettings.json";
                        File.WriteAllText(appSettingsFile, JsonConvert.SerializeObject(remoteSettings, Formatting.Indented));
                        logger.LogInformation("The appsettings.json file has been updated with the current settings.");

                        if (!string.IsNullOrEmpty(remoteSettings.AppSettings.Password))
                        {
                            logger.LogWarning("The password is not saved to the appsettings.json file.  Create a RemoteId/UserToken combination to authenticate without a password.");
                        }

                        savedSettings = true;
                    }

                    connectResult = Remote.ListenAsync(retryStarted).Result;
                }
                
                if (connectResult == DexihRemote.EConnectionResult.Disconnected)
                {
                    if (!retryStarted)
                        logger.LogWarning("Remote agent disconnected... attempting to reconnect");
                    Thread.Sleep(2000);
                }
                if (connectResult == DexihRemote.EConnectionResult.InvalidCredentials)
                {
                    logger.LogWarning("Invalid credentials... terminating service.");
                    return (int)ExitCode.InvalidLogin;
                }
                if (connectResult == DexihRemote.EConnectionResult.InvalidLocation)
                {
                    if (!retryStarted)
                        logger.LogWarning("Invalid location... web server might be down... retrying...");
                    Thread.Sleep(5000);
                }
                if(connectResult == DexihRemote.EConnectionResult.UnhandledException)
                {
                    if (!retryStarted)
                        logger.LogWarning("Unhandled exception on remote server.. retrying...");
                    Thread.Sleep(5000);
                }
                retryStarted = true;
            }
            
            // return (int)ExitCode.Terminated;
        }
    
    }
}
