using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.IO;
using Dexih.Utils.Crypto;

namespace dexih.remote
{
    public class Program
    {
        static public DexihRemote Remote { get; set; }

        public static void Main(string[] args)
        {
            // add logging.
            var loggerFactory = new LoggerFactory();
            var logger = loggerFactory.CreateLogger("dexih.remote main");

            var name = "";
            var webServer = "";
            var user = "";
            var password = "";
            var userToken = "";
            var remoteAgentId = "";
            var encryptionKey = EncryptString.GenerateRandomKey();
            Int64[] hubKeys;
            var autoSchedules = false;
            var autoUpgrade = false;
            var privacyLevel = DexihRemote.EPrivacyLevel.AllowDataDownload;
            var localDataSaveLocation = "";

            var logLevel = LogLevel.Information;

            IConfigurationSection logSection = null;
            IConfigurationSection systemSettings = null;

            //check config file first for any settings.
            if (File.Exists(Directory.GetCurrentDirectory() + "/appsettings.json"))
            {
                // Set up configuration sources.
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .AddEnvironmentVariables();

                var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
                if (environment == "Development")
                {
					builder.AddUserSecrets<Program>();
                }

                var configuration = builder.Build();

                var appSettings = configuration.GetSection("AppSettings");
                

                if (!string.IsNullOrEmpty(appSettings["User"]))
                    user = appSettings["User"];
                if (!string.IsNullOrEmpty(appSettings["WebServer"]))
                    webServer = appSettings["WebServer"];
                if (!string.IsNullOrEmpty(appSettings["Name"]))
                    name = appSettings["Name"];
                if (!string.IsNullOrEmpty(appSettings["EncryptionKey"]))
                    encryptionKey = appSettings["EncryptionKey"];
                if (!string.IsNullOrEmpty(appSettings["UserToken"]))
                    userToken = appSettings["UserToken"];
                if (!string.IsNullOrEmpty(appSettings["RemoteAgentId"]))
                    remoteAgentId = appSettings["RemoteAgentId"];
                if (!string.IsNullOrEmpty(appSettings["AutoSchedules"]))
                    autoSchedules = bool.Parse(appSettings["AutoSchedules"]);
                if (!string.IsNullOrEmpty(appSettings["AutoUpgrade"]))
                    autoUpgrade = bool.Parse(appSettings["AutoUpgrade"]);
                if (!string.IsNullOrEmpty(appSettings["PrivacyLevel"]))
                    privacyLevel = (DexihRemote.EPrivacyLevel) Enum.Parse(typeof(DexihRemote.EPrivacyLevel), appSettings["PrivacyLevel"]);
                if (!string.IsNullOrEmpty(appSettings["LocalDataSaveLocation"]))
                    localDataSaveLocation = appSettings["LocalDataSaveLocation"];

                logSection = configuration.GetSection("Logging");
                systemSettings = configuration.GetSection("SystemSettings");
            }

            //check command line for settings.  command line overrides settings file.
            for (var i = 0; i<args.Length; i++)
            {
                switch(args[i])
                {
                    case "-u":
                    case "-user":
                    case "-username":
                        i++;
                        user = args[i];
                        break;
                    case "-t":
                    case "-token":
                    case "-usertoken":
                        i++;
                        userToken = args[i];
                        break;
                    case "-w":
                    case "-webserver":
                        i++;
                        webServer = args[i];
                        break;
                    case "-p":
                    case "-password":
                        i++;
                        password = args[i];
                        break;
                    case "-k":
                    case "-key":
                        i++;
                        encryptionKey = args[i];
                        break;
                    case "-i":
                    case "-id":
                        i++;
                        remoteAgentId = args[i];
                        break;
                    case "-n":
                    case "-name":
                        i++;
                        name = args[i];
                        break;
                    case "-l":
                    case "-log":
                    case "-loglevel":
                        i++;
                        var logLevelString = args[i];
                        var checkLogLevel = Enum.TryParse<LogLevel>(logLevelString, out logLevel);
                        if (!checkLogLevel)
                        {
                            Console.WriteLine("The log level setting was not recognised.  The value was: {0}", logLevelString);
                            return;
                        }
                        break;
                    case "-h":
                    case "-hubs":
                        i++;
                        var hubs = args[i];
                        var stringHubs = hubs.Split(',');
                        long result;
                        if(stringHubs.Any(c => long.TryParse(c, out result)))
                        {
                            Console.WriteLine("The -s/-hubs option should be followed by a comma seperated list of the hubs the remote server should use.  The value was: {0} ", hubs);
                            return;
                        }
                        hubKeys = stringHubs.Select(long.Parse).ToArray();
                        break;
                    case "-a":
                    case "-activate":
                        i++;
                        var checkActivateSchedules = bool.TryParse(args[i], out autoSchedules);
                        if (!checkActivateSchedules)
                        {
                            Console.WriteLine("The -a/-activate option should be followed by \"true\" or \"false\".  The value was: {0}", args[i]);
                            return;
                        }
                        break;
                    case "-c":
                    case "-cache":
                        i++;
                        var cacheFile = args[i];
                        if(!File.Exists(cacheFile))
                        {
                            Console.WriteLine("The -c/-cache option is follows by a file that does not exist.  The file name is: {0}", args[i]);
                            return;
                        }
                        break;
                }
            }

            //add logging in priority commandline, appsettings file or use default "Information" level.
            if (logLevel != LogLevel.Information)
                loggerFactory.AddConsole(logLevel);
            else if (logSection != null)
                loggerFactory.AddConsole(logSection);
            else
                loggerFactory.AddConsole(logLevel);

            //any critical settings not received, prompt user.
            if (webServer == "")
            {
                Console.Write("Enter the dexih web server: ");
                webServer = Console.ReadLine();
            }

            if (remoteAgentId == "")
            {
                Console.Write("Enter the unique remote agent id: ");
                remoteAgentId = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(user))
            {
                Console.Write("Enter the username: ");
                user = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(userToken) && string.IsNullOrEmpty(password))
            {
                Console.Write("Enter the password: ");
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
                password = pass;
            }

            if (name == "")
            {
                name = Environment.MachineName;
            }

            logger.LogInformation("Connecting to server.  ctrl-c to terminate.");

            Remote = new DexihRemote(webServer, user, userToken, password, name, encryptionKey, remoteAgentId, privacyLevel, localDataSaveLocation, loggerFactory, systemSettings);

            //use this flag so the retrying only displays once.
            var retryStarted = false;

            while (true)
            {
                var connectResult = Remote.ConnectAsync(retryStarted).Result;

                if (connectResult == DexihRemote.EConnectionResult.Disconnected)
                {
                    if (!retryStarted)
                        logger.LogWarning("Remote agent disconnected... attempting to reconnect");
                    Thread.Sleep(2000);
                }
                if (connectResult == DexihRemote.EConnectionResult.InvalidCredentials)
                {
                    logger.LogWarning("Invalid credentials... terminating service.");
                    break;
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

        }
    }
}
