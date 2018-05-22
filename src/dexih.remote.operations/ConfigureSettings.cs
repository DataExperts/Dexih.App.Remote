using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using dexih.repository;
using Microsoft.EntityFrameworkCore.ValueGeneration.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace dexih.remote
{
    public class ConfigureSettings
    {
        const string defaultWebServer = "https://dexih.dataexpertsgroup.com";

        public RemoteSettings RemoteSettings { get; set; }
        public bool ResetSettings { get; set; }
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
            
            ResetSettings = RemoteSettings.AppSettings.FirstRun;
            SaveSettings = RemoteSettings.AppSettings.FirstRun;
        }
        
        public bool AddCommandLineValues(string[] args)
        {
             //check command line for settings.  command line overrides settings file.
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
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
                    case "-r":
                    case "-reset":
                        ResetSettings = true;
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
                        RemoteSettings.AppSettings.EnforceHttps = true;
                        break;
                    case "-certificatefilename":
                        i++;
                        RemoteSettings.AppSettings.CertificateFilename = args[i];
                        break;
                    case "-certificatepassword":
                        i++;
                        RemoteSettings.AppSettings.CertificatePassword = args[i];
                        break;
                    case "-a":
                    case "-autogeneratecertificate":
                        RemoteSettings.AppSettings.AutoGenerateCertificate = true;
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
                            $"The application will exit so an upgrade can be completed.  To skip upgrade checks include \"-skipupgrade\" in the command line, or set AutoUpgrade=false in the appsettings.json file.");
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
            
            //any critical settings not received, prompt user.
            if (ResetSettings || string.IsNullOrEmpty(RemoteSettings.AppSettings.WebServer))
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

           
            if (ResetSettings || string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
            {
                checkSaveSettings = true;

                if (string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
                {
                    Console.WriteLine($"Enter the encryption key.");
                    Console.Write($"This is used to encrypt/decrypt data marked as secure. [auto-generate]: ");
                    RemoteSettings.AppSettings.EncryptionKey = Console.ReadLine();
                    if (string.IsNullOrEmpty(RemoteSettings.AppSettings.EncryptionKey))
                    {
                        RemoteSettings.AppSettings.EncryptionKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
                        Console.WriteLine($"New encryption key \"{RemoteSettings.AppSettings.EncryptionKey}\".");
                    }
                }
                else
                {
                    string key;
                    Console.Write($"Enter the encryption key [blank - use current, \"new\" to generate]: ");
                    key = Console.ReadLine();
                    if (string.IsNullOrEmpty(key) || key.ToLower() == "new")
                    {
                        RemoteSettings.AppSettings.EncryptionKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
                        Console.WriteLine($"New encryption key \"{RemoteSettings.AppSettings.EncryptionKey}\".");
                    }
                   
                }
            }
            
            //any critical settings not received, prompt user.
            if (ResetSettings || string.IsNullOrEmpty(RemoteSettings.AppSettings.Name))
            {
                checkSaveSettings = true;

                RemoteSettings.AppSettings.AllowDataUpload = GetBoolInput(
                    "Allow files to be uploaded through the agent", RemoteSettings.AppSettings.AllowDataUpload);

                RemoteSettings.AppSettings.AllowDataDownload = GetBoolInput(
                    "Allow files to be downloaded through the agent", RemoteSettings.AppSettings.AllowDataDownload);

                if (RemoteSettings.AppSettings.AllowDataDownload || RemoteSettings.AppSettings.AllowDataUpload)
                {
                    RemoteSettings.AppSettings.DownloadDirectly = GetBoolInput(
                        "Allow direct upload/downloads (if false data will be proxied through the information hub web server)",
                        RemoteSettings.AppSettings.DownloadDirectly);

                    if (RemoteSettings.AppSettings.DownloadDirectly)
                    {
                        RemoteSettings.AppSettings.EnforceHttps = GetBoolInput(
                            "Enforce Https(encrypted) for upload/download of data", RemoteSettings.AppSettings.EnforceHttps);

                        if (RemoteSettings.AppSettings.EnforceHttps)
                        {
                            RemoteSettings.AppSettings.AutoGenerateCertificate = GetBoolInput(
                                "Automatically generate a SSL certificate",
                                RemoteSettings.AppSettings.AutoGenerateCertificate);

                            if (!RemoteSettings.AppSettings.AutoGenerateCertificate)
                            {
                                RemoteSettings.AppSettings.CertificateFilename = GetStringInput(
                                    "Enter the path/filename of the SSL (PKF) cerficiate",
                                    RemoteSettings.AppSettings.CertificateFilename, false);
                            }

                            RemoteSettings.AppSettings.CertificatePassword = GetStringInput(
                                "Enter password for the SSL certificate",
                                RemoteSettings.AppSettings.CertificatePassword, false);
                        }

                        RemoteSettings.AppSettings.DownloadPort = GetNumberInput(
                            "Enter the network port to listen for data upload/download connections",
                            RemoteSettings.AppSettings.DownloadPort, false, 1, 65535);

                        RemoteSettings.AppSettings.DownloadLocalIp = GetBoolInput(
                            "Use a local network ip address for upload/downloads (vs the public ip address that the web server sees)",
                            RemoteSettings.AppSettings.DownloadLocalIp);

                        if (!RemoteSettings.AppSettings.AutoGenerateCertificate)
                        {
                            RemoteSettings.AppSettings.ExternalDownloadUrl = GetStringInput(
                                "Enter an external url to access the http connection for data upload/download (this can be used when using a proxy server or forwarding the port to another network or internet)",
                                RemoteSettings.AppSettings.ExternalDownloadUrl, true);
                        }

                    }
                }
            }
            
            if (ResetSettings || string.IsNullOrEmpty(RemoteSettings.AppSettings.User))
            {
                checkSaveSettings = true;

                Console.Write($"Enter the login email [{RemoteSettings.AppSettings.User}]: ");
                var user = Console.ReadLine();
                if (!string.IsNullOrEmpty(user))
                {
                    RemoteSettings.AppSettings.User = user;
                }
            }

            
            if (ResetSettings || (string.IsNullOrEmpty(RemoteSettings.AppSettings.UserToken) && string.IsNullOrEmpty(RemoteSettings.Runtime.Password)))
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
                        $"Enter the user token:",
                        RemoteSettings.AppSettings.UserToken, false);
                }
                else
                {
                    // if there is a password, and reset settings has been asked, then remove the user token.
                    if (ResetSettings)
                    {
                        RemoteSettings.AppSettings.UserToken = null;
                    }
                }
            }

            if (string.IsNullOrEmpty(RemoteSettings.AppSettings.RemoteAgentId))
            {
                RemoteSettings.AppSettings.RemoteAgentId = Guid.NewGuid().ToString();
            }

            if (ResetSettings || string.IsNullOrEmpty(RemoteSettings.AppSettings.Name))
            {
                checkSaveSettings = true;

                Console.Write("Enter a name to describe this remote agent [blank use machine name]: ");
                RemoteSettings.AppSettings.Name = Console.ReadLine();
                
                if (string.IsNullOrEmpty(RemoteSettings.AppSettings.Name))
                {
                    RemoteSettings.AppSettings.Name = Environment.MachineName;
                }
            }
            
            if (ResetSettings || (checkSaveSettings && !SaveSettings))
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
                    SaveSettings = false;
                }
                else if(saveResult == "yes")
                {
                    SaveSettings = true;
                }
            }
        }

      
    }
}