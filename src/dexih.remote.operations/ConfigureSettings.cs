using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using dexih.repository;
using Dexih.Utils.Crypto;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace dexih.remote
{
    public class ConfigureSettings
    {
        const string defaultWebServer = "https://dexih.dataexpertsgroup.com";

        public RemoteSettings RemoteSettings { get; set; }
        public bool SaveSettings { get; set; }
        public ILogger Logger { get; set; }

        /// <summary>
        /// Loads settings from the configuration file or environment variables, or user secrets (developement only)
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="remoteSettings"></param>
        /// <returns></returns>
        public ConfigureSettings(ILogger logger, RemoteSettings remoteSettings)
        {
            Logger = logger;

            RemoteSettings = remoteSettings;
            
            SaveSettings = RemoteSettings.AppSettings.UserPrompt;
        }
        
        public bool AddCommandLineValues(string[] args)
        {
             //check command line for settings.  command line overrides settings file.
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
                    case "-appsettings":
                        i++;
                        break;
                    case "-u":
                    case "-user":
                    case "-username":
                        i++;
                        RemoteSettings.AppSettings.User = args[i];
                        break;
                    case "-t":
                    case "-token":
                    case "-usertoken":
                        i++;
                        RemoteSettings.AppSettings.UserToken = args[i];
                        break;
                    case "-w":
                    case "-webserver":
                        i++;
                        RemoteSettings.AppSettings.WebServer = args[i];
                        break;
                    case "-p":
                    case "-password":
                        i++;
                        RemoteSettings.Runtime.Password = args[i];
                        break;
                    case "-k":
                    case "-key":
                        i++;
                        RemoteSettings.AppSettings.EncryptionKey = args[i];
                        break;
                    case "-i":
                    case "-id":
                        i++;
                        RemoteSettings.AppSettings.RemoteAgentId = args[i];
                        break;
                    case "-n":
                    case "-name":
                        i++;
                        RemoteSettings.AppSettings.Name = args[i];
                        break;
                    case "-l":
                    case "-log":
                    case "-loglevel":
                        i++;
                        var logLevelString = args[i];
                        var checkLogLevel = Enum.TryParse<LogLevel>(logLevelString, out var logLevel);
                        if (!checkLogLevel)
                        {
                            Logger.LogError("The log level setting was not recognised.  The value was: {0}", logLevelString);
                            return false;
                        }
                        RemoteSettings.Logging.LogLevel.Default = logLevel;
                        break;
                    case "-s":
                    case "-save":
                        SaveSettings = true;
                        break;
                    case "-prompt":
                    case "-userprompt":
                        RemoteSettings.AppSettings.UserPrompt = true;
                        break;
                    case "-up":
                    case "-upgrade":
                        RemoteSettings.AppSettings.AutoUpgrade = true;
                        break;
                    case "-skipupgrade":
                        RemoteSettings.AppSettings.AutoUpgrade = false;
                        break;
                    case "-pr":
                    case "-prerelease":
                        RemoteSettings.AppSettings.AllowPreReleases = true;
                        break;
                    case "-https":
                    case "-ssl":
                    case "-enforsehttps":
                        RemoteSettings.Network.EnforceHttps = true;
                        break;
                    case "-disablelanaccess":
                        RemoteSettings.Privacy.AllowLanAccess = false;
                        break;
                    case "-disableaxternalaccess":
                        RemoteSettings.Privacy.AllowExternalAccess = false;
                        break;
                    case "-disableproxyaccess":
                        RemoteSettings.Privacy.AllowProxy = false;
                        break;
                    case "-certificatefilename":
                        i++;
                        RemoteSettings.Network.CertificateFilename = args[i];
                        break;
                    case "-certificatepassword":
                        i++;
                        RemoteSettings.Network.CertificatePassword = args[i];
                        break;
                    case "-a":
                    case "-autogeneratecertificate":
                        RemoteSettings.Network.AutoGenerateCertificate = true;
                        break;
                    case "-disableupnp":
                        RemoteSettings.Network.EnableUPnP = false;
                        break;
                    default:
                        Logger.LogError($"The command line option {args[i]} was not recognised.");
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Checks for a newer release, and downloads if there is.
        /// </summary>
        /// <returns>True is upgrade is required.</returns>
        public async Task<bool> CheckUpgrade()
        {
            // check if there is a newer release.
            if (RemoteSettings.AppSettings.AutoUpgrade)
            {
                string downloadUrl = null;
                string latestVersion = null;

                try
                {
                    using (var httpClient = new HttpClient())
                    {
                        httpClient.DefaultRequestHeaders.Add("User-Agent", "Dexih Remote Agent");
                        JToken jToken;
                        if (RemoteSettings.AppSettings.AllowPreReleases)
                        {
                            // this api gets all releases.
                            var response =
                                await httpClient.GetAsync(
                                    "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases");
                            var responseText = await response.Content.ReadAsStringAsync();
                            var releases = JArray.Parse(responseText);
                            // the first release will be the latest.
                            jToken = releases[0];
                        }
                        else
                        {
                            // this api gets the latest release, excluding prereleases.
                            var response =
                                await httpClient.GetAsync(
                                    "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases/latest");
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

                        // Download and save the update
//                        if (!string.IsNullOrEmpty(downloadUrl))
//                        {
//                            Logger.LogInformation($"Downloading latest remote agent release from {downloadUrl}.");
//                            var releaseFileName = Path.Combine(Path.GetTempPath(), "dexih.remote.latest.zip");
//
//                            if (File.Exists(releaseFileName))
//                            {
//                                File.Delete(releaseFileName);
//                            }
//
//                            using (var response = await httpClient.GetAsync(downloadUrl, HttpCompletionOption.ResponseHeadersRead))
//                            using (var streamToReadFrom = await response.Content.ReadAsStreamAsync())
//                            {
//                                using (Stream streamToWriteTo = File.Open(releaseFileName, FileMode.Create))
//                                {
//                                    await streamToReadFrom.CopyToAsync(streamToWriteTo);
//                                }
//                            }
//
//                            var extractDirectory = Path.Combine(Path.GetTempPath(), "remote.agent");
//                            if (Directory.Exists(extractDirectory))
//                            {
//                                Directory.Delete(extractDirectory, true);
//                            }
//
//                            Directory.CreateDirectory(extractDirectory);
//                            ZipFile.ExtractToDirectory(releaseFileName, extractDirectory);
//                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex,
                        "There was an issue getting the latest release url from github.  Error: " + ex.Message);
                }

                if (string.IsNullOrEmpty(downloadUrl))
                {
                    Logger.LogError("There was an issue getting the latest release url from github.");
                }
                else
                {
                    var localVersion = Assembly.GetEntryAssembly()
                        .GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

                    var latestBuild = latestVersion.Split('-').Last();
                    var localBuild = localVersion.Split('-').Last();

                    Logger.LogWarning($"The local build is {localBuild}.");
                    Logger.LogWarning($"The latest build is {latestBuild}.");

                    if (string.CompareOrdinal(latestBuild, localBuild) > 0)
                    {
                        File.WriteAllText("latest_version.txt", latestVersion + "\n" + downloadUrl);
                        Logger.LogWarning($"The local version of the remote agent is v{localVersion}.");
                        Logger.LogWarning($"The latest version of the remote agent is {latestVersion}.");
                        Logger.LogWarning($"There is a newer release of the remote agent available at {downloadUrl}.");
                        Logger.LogWarning(
                            "The application will exit so an upgrade can be completed.  To skip upgrade checks include \"-skipupgrade\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");
                        return true;
                    }
                }
            }

            return false;
        }

        private bool GetBoolInput(string message, bool defaultValue)
        {
            var result = defaultValue;

            Console.Write($"{message} (true/false): [{defaultValue}]: ");
            var value = Console.ReadLine();

            if (!string.IsNullOrEmpty(value))
            {
                while (value != "true" && value != "false")
                {
                    Console.Write("Enter true/false: ");
                    value = Console.ReadLine();
                }
                result = Convert.ToBoolean(value);
            }
            return result;
        }
        
        private string GetStringInput(string message, string defaultValue, bool allowNull)
        {
            var result = defaultValue;

            Console.Write($"{message}: [{defaultValue}]: ");
            var inputValue = Console.ReadLine();

            if (!string.IsNullOrEmpty(inputValue))
            {
                result = inputValue;
            }
            else if (string.IsNullOrEmpty(inputValue) && string.IsNullOrEmpty(defaultValue) && !allowNull)
            {
                while (string.IsNullOrEmpty(inputValue))
                {
                    Console.Write("Enter a (non empty) value: ");
                    result = Console.ReadLine();
                }
            }

            return result;
        }
        
        private int? GetNumberInput(string message, int? defaultValue, bool allowNull, int? minValue = null, int? maxValue = null)
        {
            var result = defaultValue;

            Console.Write($"{message}: [{defaultValue}]: ");
            var inputValue = Console.ReadLine();

            while (true)
            {
                if (string.IsNullOrEmpty(inputValue) && result == null && allowNull)
                {
                    return null;
                }

                if (string.IsNullOrEmpty(inputValue) && result != null)
                {
                    return result;
                }

                if (inputValue != null && int.TryParse(inputValue, out var numValue) && numValue >= minValue &&
                    minValue <= maxValue)
                {
                    return numValue;
                }
                
                Console.Write("Enter numeric value ");
                if (minValue != null)
                {
                    Console.Write($" >{minValue} ");
                }

                if (maxValue != null)
                {
                    Console.Write($" <{maxValue}");
                }
                Console.Write(": ");
                inputValue = Console.ReadLine();
            }
        }
        
        public void GetUserInput()
        {
            var checkSaveSettings = false;
            var detailed = false;
            
            if (RemoteSettings.RequiresUserInput())
            {
                detailed = GetBoolInput("Would you like to enter detailed configuration options?", false);

                var sslConfigureComplete = false;

                if (detailed)
                {
                    //any critical settings not received, prompt user.
                    if (string.IsNullOrEmpty(RemoteSettings.AppSettings.WebServer))
                    {
                        checkSaveSettings = true;

                        RemoteSettings.AppSettings.WebServer = defaultWebServer;

                        Console.Write($"Enter the dexih web server [{RemoteSettings.AppSettings.WebServer}]: ");
                        var webServer = Console.ReadLine();

                        if (!string.IsNullOrEmpty(webServer))
                        {
                            RemoteSettings.AppSettings.WebServer = webServer;
                        }
                    }


                    if (string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
                    {
                        checkSaveSettings = true;

                        if (string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
                        {
                            Console.WriteLine("Enter the encryption key.");
                            Console.Write("This is used to encrypt/decrypt data marked as secure. [auto-generate]: ");
                            RemoteSettings.AppSettings.EncryptionKey = Console.ReadLine();
                            if (string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
                            {
                                RemoteSettings.AppSettings.EncryptionKey =
                                    EncryptString.GenerateRandomKey();
                                Console.WriteLine(
                                    $"New encryption key \"{RemoteSettings.AppSettings.EncryptionKey}\".");
                            }
                        }
                        else
                        {
                            Console.Write("Enter the encryption key [blank - use current, \"new\" to generate]: ");
                            var key = Console.ReadLine();
                            if (string.IsNullOrEmpty(key) || key.ToLower() == "new")
                            {
                                RemoteSettings.AppSettings.EncryptionKey =
                                    EncryptString.GenerateRandomKey();
                                Console.WriteLine(
                                    $"New encryption key \"{RemoteSettings.AppSettings.EncryptionKey}\".");
                            }
                        }
                    }

                    //any critical settings not received, prompt user.
                    if (string.IsNullOrEmpty(RemoteSettings.AppSettings.Name))
                    {
                        checkSaveSettings = true;

                        RemoteSettings.Privacy.AllowDataUpload = GetBoolInput(
                            "Allow files to be uploaded through the agent", RemoteSettings.Privacy.AllowDataUpload);

                        RemoteSettings.Privacy.AllowDataDownload = GetBoolInput(
                            "Allow files to be downloaded through the agent",
                            RemoteSettings.Privacy.AllowDataDownload);

                        if (RemoteSettings.Privacy.AllowDataDownload || RemoteSettings.Privacy.AllowDataUpload)
                        {
                            RemoteSettings.Network.EnforceHttps = GetBoolInput(
                                "Enforce Https(encrypted) for upload/download of data",
                                RemoteSettings.Network.EnforceHttps);

                            RemoteSettings.Privacy.AllowLanAccess = GetBoolInput(
                                "Allow direct connections through the LAN",
                                RemoteSettings.Privacy.AllowLanAccess);

                            RemoteSettings.Privacy.AllowExternalAccess = GetBoolInput(
                                "Allow direct connections externally through internet (note: ports must be mapped to internet IP)?",
                                RemoteSettings.Privacy.AllowExternalAccess);

                            if (RemoteSettings.Privacy.AllowLanAccess || RemoteSettings.Privacy.AllowExternalAccess)
                            {
                                RemoteSettings.Network.DownloadPort = GetNumberInput(
                                    "Enter the network port to listen for data upload/download connections",
                                    RemoteSettings.Network.DownloadPort, false, 1, 65535);
                                
                                RemoteSettings.Network.EnableUPnP = GetBoolInput(
                                    "Enable UPnP discovery to map port automatically to the internet?",
                                    RemoteSettings.Network.EnableUPnP);
                            }

                            RemoteSettings.Network.EnforceHttps = GetBoolInput(
                                "Allow connections through a proxy server?",
                                RemoteSettings.Privacy.AllowProxy);

                            if (RemoteSettings.Privacy.AllowProxy)
                            {
                                RemoteSettings.Network.ProxyUrl = GetStringInput(
                                    "Enter a custom proxy url (leave blank for default internet based proxy)",
                                    RemoteSettings.Network.ProxyUrl, true);
                            }

                            if (RemoteSettings.Network.EnforceHttps && (RemoteSettings.Privacy.AllowLanAccess || RemoteSettings.Privacy.AllowExternalAccess))
                            {
                                
                                RemoteSettings.Network.AutoGenerateCertificate = GetBoolInput(
                                    "Automatically generate a SSL certificate",
                                    RemoteSettings.Network.AutoGenerateCertificate);

                                if (!RemoteSettings.Network.AutoGenerateCertificate)
                                {
                                    RemoteSettings.Network.ExternalDownloadUrl = GetStringInput(
                                        "Enter an external url to access the http connection for data upload/download (this can be used when using a proxy server or forwarding the port to another network or internet)",
                                        RemoteSettings.Network.ExternalDownloadUrl, true);

                                    RemoteSettings.Network.CertificateFilename = GetStringInput(
                                        "Enter the path/filename of the SSL (PKF) cerficiate",
                                        RemoteSettings.Network.CertificateFilename, false);
                                }

                                RemoteSettings.Network.CertificatePassword = GetStringInput(
                                    "Enter password for the SSL certificate",
                                    RemoteSettings.Network.CertificatePassword, false);
                                
                                sslConfigureComplete = true;
                            }

                        }
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
                    {
                        RemoteSettings.AppSettings.EncryptionKey = EncryptString.GenerateRandomKey();
                    }

                    if (RemoteSettings.Network.AutoGenerateCertificate)
                    {
                        if (string.IsNullOrEmpty(RemoteSettings.Network.CertificateFilename))
                        {
                            RemoteSettings.Network.CertificateFilename = "dexih.pfx";
                        }

                        if (string.IsNullOrEmpty(RemoteSettings.Network.CertificatePassword))
                        {
                            if (File.Exists(RemoteSettings.Network.CertificateFilePath()))
                            {
                                RemoteSettings.Network.CertificatePassword = GetStringInput(
                                    $"Enter the password for the certificate with the name {RemoteSettings.Network.CertificateFilename}.",
                                    RemoteSettings.Network.CertificatePassword, false);
                            }
                            else
                            {
                                RemoteSettings.Network.CertificatePassword = EncryptString.GenerateRandomKey();
                            }
                        }
                    }
                }

                if (string.IsNullOrEmpty(RemoteSettings.AppSettings.User))
                {
                    checkSaveSettings = true;

                    Console.Write($"Enter the login email [{RemoteSettings.AppSettings.User}]: ");
                    var user = Console.ReadLine();
                    if (!string.IsNullOrEmpty(user))
                    {
                        RemoteSettings.AppSettings.User = user;
                    }
                }


                if ((string.IsNullOrEmpty(RemoteSettings.AppSettings.UserToken) &&
                                      string.IsNullOrEmpty(RemoteSettings.Runtime.Password)))
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
                    RemoteSettings.Runtime.Password = pass;

                    if (string.IsNullOrEmpty(RemoteSettings.Runtime.Password))
                    {
                        RemoteSettings.AppSettings.RemoteAgentId = GetStringInput(
                            $"Enter the unique remote agent id (create new tokens at: {RemoteSettings.AppSettings.WebServer}/hubs/index/remoteAgents:",
                            RemoteSettings.AppSettings.RemoteAgentId, false);

                        RemoteSettings.AppSettings.UserToken = GetStringInput(
                            "Enter the user token:",
                            RemoteSettings.AppSettings.UserToken, false);
                    }
                    else
                    {
                        // if there is a password, and reset settings has been asked, then remove the user token.
                        if (RemoteSettings.AppSettings.UserPrompt)
                        {
                            RemoteSettings.AppSettings.UserToken = null;
                        }
                    }
                }

                if (string.IsNullOrEmpty(RemoteSettings.AppSettings.RemoteAgentId))
                {
                    RemoteSettings.AppSettings.RemoteAgentId = Guid.NewGuid().ToString();
                }

                if (string.IsNullOrEmpty(RemoteSettings.AppSettings.Name))
                {
                    checkSaveSettings = true;

                    Console.Write($"Enter a name to describe this remote agent [{Environment.MachineName}]: ");
                    RemoteSettings.AppSettings.Name = Console.ReadLine();

                    if (string.IsNullOrEmpty(RemoteSettings.AppSettings.Name))
                    {
                        RemoteSettings.AppSettings.Name = Environment.MachineName;
                    }
                }

                if (!sslConfigureComplete)
                {
                    if(RemoteSettings.Network.AutoGenerateCertificate && string.IsNullOrEmpty(RemoteSettings.Network.CertificateFilename))
                    {
                        RemoteSettings.Network.CertificateFilename = "dexih.pfx";
                        checkSaveSettings = true;
                    }

                    if (File.Exists(RemoteSettings.Network.CertificateFilename) && string.IsNullOrEmpty(RemoteSettings.Network.CertificatePassword))
                    {
                        RemoteSettings.Network.CertificatePassword = GetStringInput(
                            $"An SSL certificate {RemoteSettings.Network.CertificateFilename} was found.  Enter the password for this certificate: ",
                            RemoteSettings.Network.CertificatePassword, false);
                        checkSaveSettings = true;
                    }
                }

                if (checkSaveSettings && !SaveSettings)
                {
                    Console.Write("Would you like to save settings (enter yes or no) [no]?: ");
                    var saveResult = Console.ReadLine().ToLower();

                    while (!(saveResult == "yes" || saveResult == "no" || string.IsNullOrEmpty(saveResult)))
                    {
                        Console.Write("Would you like to save settings (enter yes or no) [no]?: ");
                        saveResult = Console.ReadLine().ToLower();
                    }

                    if (saveResult == "no" || saveResult == "no" || string.IsNullOrEmpty(saveResult))
                    {
                        SaveSettings = false;
                    }
                    else if (saveResult == "yes")
                    {
                        SaveSettings = true;
                    }
                }
            }
            else
            {
                Logger.LogInformation("Automatically logging in.  To re-enter configuration run with the \"-reset\" flag.");
            }
        }

      
    }
}