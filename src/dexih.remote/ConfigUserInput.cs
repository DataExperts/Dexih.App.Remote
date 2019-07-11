using System;
using System.Collections.Generic;
using System.IO;
using dexih.repository;
using Dexih.Utils.Crypto;
using Microsoft.Extensions.Configuration;

namespace dexih.remote.config
{
    public static class ConfigUserInput
    {
        public static IConfigurationBuilder AddUserInput(this IConfigurationBuilder configurationBuilder)
        {
            const string defaultWebServer = "https://dexih.com";

            var checkSaveSettings = false;

            var configBuild = configurationBuilder.Build();
            var remoteSettings = configBuild.Get<RemoteSettings>() ?? new RemoteSettings();

            var newSettings = new Dictionary<string, string>();
            
            if (remoteSettings.RequiresUserInput())
            {
                var detailed = GetBoolInput("Would you like to enter detailed configuration options?", false);

                var sslConfigureComplete = false;

                if (detailed)
                {
                    checkSaveSettings = true;
                    
                    if (string.IsNullOrEmpty(remoteSettings.AppSettings.EncryptionKey))
                    {
                        Console.WriteLine("Enter the encryption key.");
                        Console.Write("This is used to encrypt/decrypt data marked as secure. [auto-generate]: ");
                        var encryptionKey = Console.ReadLine();
                        if (string.IsNullOrEmpty(encryptionKey))
                        {
                            newSettings["AppSettings:EncryptionKey"] = EncryptString.GenerateRandomKey();
                        }
                        else
                        {
                            newSettings["AppSettings:EncryptionKey"] = encryptionKey;
                        }
                        Console.WriteLine($"New encryption key \"{newSettings["AppSettings:EncryptionKey"]}\".");
                    }
                    else
                    {
                        Console.Write("Enter the encryption key [blank - use current, \"new\" to generate]: ");
                        var encryptionKey = Console.ReadLine();
                        if (string.IsNullOrEmpty(encryptionKey))
                        {
                            Console.WriteLine($"Existing key used: \"{remoteSettings.AppSettings.EncryptionKey}\".");
                        }
                        if ( encryptionKey.ToLower() == "new")
                        {
                            newSettings["AppSettings:EncryptionKey"] =EncryptString.GenerateRandomKey();
                            Console.WriteLine($"New Encryption key \"{newSettings["AppSettings:EncryptionKey"]}\".");
                        }
                    }

                    //any critical settings not received, prompt user.
                    var allowDataUpload = GetBoolInput(
                        "Allow files to be uploaded through the agent", remoteSettings.Privacy.AllowDataUpload);
                    newSettings["Privacy:AllowDataUpload"] = allowDataUpload.ToString();

                    var allowDataDownload = GetBoolInput(
                        "Allow files to be downloaded through the agent",
                        remoteSettings.Privacy.AllowDataDownload);
                    newSettings["Privacy:AllowDataDownload"] = allowDataDownload.ToString();

                    if (allowDataDownload || allowDataUpload)
                    {
                        newSettings["Network:EnforceHttps"] = GetBoolInput(
                            "Enforce Https(encrypted) for upload/download of data",
                            remoteSettings.Network.EnforceHttps).ToString();

                        var allowLanAccess = GetBoolInput(
                            "Allow direct connections through the LAN",
                            remoteSettings.Privacy.AllowLanAccess);
                        newSettings["Privacy:AllowLanAccess"] = allowLanAccess.ToString();

                        var allowExternalAccess = GetBoolInput(
                            "Allow direct connections externally through internet (note: ports must be mapped to internet IP)?",
                            remoteSettings.Privacy.AllowExternalAccess);
                        newSettings["Privacy:AllowExternalAccess"] = allowExternalAccess.ToString();
                        
                        if (allowLanAccess || allowExternalAccess)
                        {
                            newSettings["Network:DownloadPort"] = GetNumberInput(
                                "Enter the network port to listen for data upload/download connections",
                                remoteSettings.Network.DownloadPort, false, 1, 65535).ToString();

                            newSettings["Network:EnableUPnP"] = GetBoolInput(
                                "Enable UPnP discovery to map port automatically to the internet?",
                                remoteSettings.Network.EnableUPnP).ToString();
                        }

                        var enforceHttps = GetBoolInput(
                            "Allow connections through a proxy server?",
                            remoteSettings.Network.EnforceHttps);

                        newSettings["Network:EnforceHttps"] = enforceHttps.ToString();

                        var allowProxyAccess = GetBoolInput(
                            "Allow connections externally through a proxy server?",
                            remoteSettings.Privacy.AllowExternalAccess);
                        newSettings["Privacy:AllowProxy"] = allowProxyAccess.ToString();

                        if (allowProxyAccess)
                        {
                            newSettings["Network:ProxyUrl"] = GetStringInput(
                                "Enter a custom proxy url (leave blank for default internet based proxy)",
                                remoteSettings.Network.ProxyUrl, true);
                        }

                        if (enforceHttps &&
                            (allowLanAccess || allowExternalAccess))
                        {

                            var autoGenerateCertificate = GetBoolInput(
                                "Automatically generate a SSL certificate",
                                remoteSettings.Network.AutoGenerateCertificate);

                            newSettings["Network:AutoGenerateCertificate"] = autoGenerateCertificate.ToString();

                            if (autoGenerateCertificate)
                            {
                                newSettings["Network:ExternalDownloadUrl"] = GetStringInput(
                                    "Enter an external url to access the http connection for data upload/download (this can be used when using a proxy server or forwarding the port to another network or internet)",
                                    remoteSettings.Network.ExternalDownloadUrl, true);

                                newSettings["Network:CertificateFilename"] = GetStringInput(
                                    "Enter the path/filename of the SSL (PKF) certificate",
                                    remoteSettings.Network.CertificateFilename, false);
                            }

                            newSettings["Network:CertificatePassword"] = GetStringInput(
                                "Enter password for the SSL certificate",
                                remoteSettings.Network.CertificatePassword, false);

                            sslConfigureComplete = true;
                        }

                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(remoteSettings.AppSettings.EncryptionKey))
                    {
                        newSettings["AppSettings:EncryptionKey"] = EncryptString.GenerateRandomKey();
                    }

                    if (remoteSettings.Network.AutoGenerateCertificate)
                    {
                        if (string.IsNullOrEmpty(remoteSettings.Network.CertificateFilename))
                        {
                            newSettings["Network:CertificateFilename"] = "dexih.pfx";
                        }

                        if (string.IsNullOrEmpty(remoteSettings.Network.CertificatePassword))
                        {
                            if (File.Exists(remoteSettings.CertificateFilePath()))
                            {
                                newSettings["Network:CertificatePassword"] = GetStringInput(
                                    $"Enter the password for the certificate with the name {remoteSettings.Network.CertificateFilename}.",
                                    remoteSettings.Network.CertificatePassword, false);
                            }
                            else
                            {
                                newSettings["Network:CertificatePassword"] = EncryptString.GenerateRandomKey();
                            }
                        }
                    }
                }

                checkSaveSettings = true;
                
                newSettings["AppSettings:WebServer"] = defaultWebServer;

                Console.Write($"Enter the dexih web server [{remoteSettings.AppSettings.WebServer}]: ");
                var webServer = Console.ReadLine();

                if (!string.IsNullOrEmpty(webServer))
                {
                    newSettings["AppSettings:WebServer"] = webServer;
                }

                Console.Write($"Enter the login email [{remoteSettings.AppSettings.User}]: ");
                var user = Console.ReadLine();
                if (!string.IsNullOrEmpty(user))
                {
                    newSettings["AppSettings:User"] = user;
                }

                Console.Write("Enter the password [leave empty to specify user token]: ");
                ConsoleKeyInfo keyInfo;
                var pass = "";
                do
                {
                    keyInfo = Console.ReadKey(true);
                    if (keyInfo.Key != ConsoleKey.Backspace && keyInfo.Key != ConsoleKey.Enter)
                    {
                        pass += keyInfo.KeyChar;
                        Console.Write("*");
                    }
                    else
                    {
                        if (keyInfo.Key == ConsoleKey.Backspace && pass.Length > 0)
                        {
                            pass = pass.Substring(0, (pass.Length - 1));
                            Console.Write("\b \b");
                        }
                    }
                } while (keyInfo.Key != ConsoleKey.Enter);

                Console.WriteLine();
                newSettings["Runtime:Password"] = pass;

                var remoteAgentId = remoteSettings.AppSettings.RemoteAgentId;
                
                if (string.IsNullOrEmpty(pass))
                {
                    remoteAgentId = GetStringInput(
                        $"Enter the unique remote agent id (create new tokens at: {remoteSettings.AppSettings.WebServer}/hubs/index/remoteAgents:",
                        remoteSettings.AppSettings.RemoteAgentId, false);

                    newSettings["AppSettings:RemoteAgentId"] = remoteAgentId;

                    newSettings["AppSettings:UserToken"] = GetStringInput(
                        "Enter the user token:",
                        remoteSettings.AppSettings.UserToken, false);
                }
                else
                {
                    // if there is a password, and reset settings has been asked, then remove the user token.
                    if (remoteSettings.AppSettings.UserPrompt)
                    {
                        newSettings["AppSettings:UserToken"] = null;
                    }
                }
                
                if (string.IsNullOrEmpty(remoteAgentId))
                {
                    newSettings["AppSettings:RemoteAgentId"] = Guid.NewGuid().ToString();
                }

                Console.Write($"Enter a name to describe this remote agent [{Environment.MachineName}]: ");
                newSettings["AppSettings:Name"] = Console.ReadLine();

                if (string.IsNullOrEmpty(remoteSettings.AppSettings.Name))
                {
                    newSettings["AppSettings:Name"] = Environment.MachineName;
                }

                if (!sslConfigureComplete)
                {
                    var certificateFileName = remoteSettings.Network.CertificateFilename;
                    
                    if(remoteSettings.Network.AutoGenerateCertificate && string.IsNullOrEmpty(certificateFileName))
                    {
                        certificateFileName = "dexih.pfx";
                        newSettings["Network:CertificateFilename"] = certificateFileName;
                        checkSaveSettings = true;
                    }

                    if (File.Exists(certificateFileName) && string.IsNullOrEmpty(remoteSettings.Network.CertificatePassword))
                    {
                        newSettings["Network:CertificatePassword"] = GetStringInput(
                            $"An SSL certificate {certificateFileName} was found.  Enter the password for this certificate: ",
                            remoteSettings.Network.CertificatePassword, false);
                        checkSaveSettings = true;
                    }
                }

                if (checkSaveSettings && ! remoteSettings.Runtime.SaveSettings)
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
                        newSettings["Runtime:SaveSettings"] = "false";
                    }
                    else if (saveResult == "yes")
                    {
                        newSettings["Runtime:SaveSettings"] = "true";
                    }
                }
            }

            configurationBuilder.AddInMemoryCollection(newSettings);

            return configurationBuilder;
        }
        
        private static bool GetBoolInput(string message, bool defaultValue)
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
        
        private static string GetStringInput(string message, string defaultValue, bool allowNull)
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
                    inputValue = Console.ReadLine();
                }

                result = inputValue;
            }

            return result;
        }
        
        private static int? GetNumberInput(string message, int? defaultValue, bool allowNull, int? minValue = null, int? maxValue = null)
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
    }
}