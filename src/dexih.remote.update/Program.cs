

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace dexih.remote.update
{
    class Program
    {
       
        static void Main(string[] args)
        {
            Welcome();
            
            bool allowPreRelease = false;
            int waitProcess = 0;
            var path = "dexih.remote";

            foreach (var arg in args)
            {
                if (arg == "PRE")
                {
                    allowPreRelease = true;
                }

                var argSplit = arg.Split('=');


                if (argSplit.Length == 2)
                {
                    switch (argSplit[0].ToUpper())
                    {
                        case "PROCESS":
                            waitProcess = Convert.ToInt32(argSplit[1]);
                            break;
                        case "PATH":
                            path = argSplit[1];
                            break;
                            
                    }
                }
            }

            var localVersion = "v0.0.0";
            var latestVersion = "v0.0.0";
            var latestName = "";
            string downloadUrl = null;

            var remoteAgentName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? "dexih.remote.exe"
                : "dexih.remote";

            var remoteAgentPath = Path.Combine(path, remoteAgentName);

            var remoteAgentExists = Directory.Exists(path) && File.Exists(remoteAgentPath);

            if (remoteAgentExists)
            {
                var fullpath = Path.Combine(Directory.GetCurrentDirectory(), path, "dexih.remote.dll");
                localVersion = "v" + AssemblyName.GetAssemblyName(fullpath).Version.ToString();
                Console.WriteLine($"*** Current remote agent version is {localVersion} ***");
            }
            else
            {
                Console.WriteLine($"*** Installing a remote agent instance ***");
            }

            JToken jToken;
            
            using (var httpClient = new HttpClient())
            {
                httpClient.DefaultRequestHeaders.Add("User-Agent", "Dexih Remote Agent");
                
                if (allowPreRelease)
                {
                    // this api gets all releases.
                    var response =
                        httpClient.GetAsync(
                            "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases").Result;

                    if (!response.IsSuccessStatusCode)
                    {
                        Console.Error.WriteLine("The github release api could not be contacted.  Reason: " +
                                                response.ReasonPhrase);
                        return;
                    }

                    try
                    {
                        var responseText = response.Content.ReadAsStringAsync().Result;
                        var releases = JArray.Parse(responseText);
                        // the first release will be the latest.
                        jToken = releases[0];
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("There was an error getting the latest version information.  Reason: " +
                                                ex.Message);
                        return;
                    }
                }
                else
                {
                    // this api gets the latest release, excluding pre-releases.
                    var response =
                        httpClient.GetAsync(
                            "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases/latest").Result;

                    if (!response.IsSuccessStatusCode)
                    {
                        Console.Error.WriteLine("The github release api could not be contacted.  Reason: " +
                                                response.ReasonPhrase);
                        return;
                    }

                    try
                    {
                        var responseText = response.Content.ReadAsStringAsync().Result;
                        jToken = JToken.Parse(responseText);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine("There was an error getting the latest version information.  Reason: " +
                                                ex.Message);
                        return;
                    }
                }
            }

            latestVersion = (string) jToken["tag_name"];
            foreach (var asset in jToken["assets"])
            {
                latestName = ((string) asset["name"]).ToLower();
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && latestName.Contains("windows"))
                {
                    downloadUrl = (string) asset["browser_download_url"];
                    break;
                }

                if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) && latestName.Contains("osx"))
                {
                    downloadUrl = (string) asset["browser_download_url"];
                    break;
                }

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && latestName.Contains("linux"))
                {
                    downloadUrl = (string) asset["browser_download_url"];
                    break;
                }
            }

            if (string.IsNullOrEmpty(downloadUrl))
            {
                Console.Error.WriteLine("There was an error getting the latest download Url.");
                return;
            }

            if (string.CompareOrdinal(latestVersion, localVersion) > 0)
            {

                Console.WriteLine($"*** Upgrading from version {localVersion} to {latestVersion}.");
                Console.WriteLine($"*** Downloading the latest version from {downloadUrl} ***");

                using (var progress = new ProgressBar())
                {
                    var completeEvent = new ManualResetEvent(false);

                    //web client call back procedures
                    void DownloadProgressCallback(object sender, DownloadProgressChangedEventArgs e)
                    {
                        progress.Report(e.ProgressPercentage / 100.0);
                    }

                    //web client call back procedures
                    void DownloadProgressComplete(object sender, AsyncCompletedEventArgs e)
                    {
                        if (e.Cancelled)
                        {
                            throw new Exception("The download was cancelled");
                        }

                        if (e.Error != null)
                        {
                            throw e.Error;
                        }

                        completeEvent.Set();
                    }

                    using (var client = new WebClient())
                    {
                        try
                        {
                            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

                            // Specify a progress notification handler.
                            client.DownloadProgressChanged += DownloadProgressCallback;
                            client.DownloadFileCompleted += DownloadProgressComplete;
                            client.DownloadFileAsync(new Uri(downloadUrl), latestName);
                            completeEvent.WaitOne();
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine(
                                $"Could not download the latest release at {downloadUrl}.  Reason: " +
                                ex.Message);
                            return;
                        }
                    }
                }

                var tempDirectory = "remote_agent_" + Guid.NewGuid();
                Directory.CreateDirectory(tempDirectory);

                ZipFile.ExtractToDirectory(latestName, tempDirectory);

                if (!File.Exists(Path.Combine(tempDirectory, remoteAgentName)))
                {
                    Console.Error.WriteLine(
                        $"There was an issue with the downloaded file.  The file {remoteAgentName} was not found.");
                    return;
                }

                if (waitProcess > 0)
                {
                    var process = Process.GetProcessById(waitProcess);
                    if (process != null)
                    {
                        Console.WriteLine(
                            $"Waiting for the process {waitProcess} to finish, before completing update.");
                        process.WaitForExit();
                    }
                }

                if (Directory.Exists(path))
                {
                    var backupPath = DateTime.Now.ToString("yyyyMMddHHmmssfff");
                    Directory.Move(path, path + "_" + DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                    Console.WriteLine($"*** Backed up the old remote agent binaries to {backupPath} ");

                }

                Directory.Move(tempDirectory, path);

                // modify the execute permissions, as the .net core unzip fails to set permissions correctly.
                if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var process = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            RedirectStandardOutput = true,
                            UseShellExecute = false,
                            CreateNoWindow = true,
                            WindowStyle = ProcessWindowStyle.Hidden,
                            FileName = "chmod",
                            Arguments = $"+x {remoteAgentPath}"
                        }
                    };

                    process.Start();
                    process.WaitForExit();

                }
            }
            else
            {
                Console.WriteLine("*** The current version is the latest version ***");
            }
            
            Console.WriteLine("*** Starting the remote agent ***");
            
            var remoteProcess = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    CreateNoWindow = false,
                    ErrorDialog = false,
                    RedirectStandardError = false,
                    RedirectStandardOutput = false,
                    RedirectStandardInput = false,
                    UseShellExecute = false,
                    FileName = remoteAgentPath,
                    WindowStyle = ProcessWindowStyle.Hidden,
                    }
            };
            remoteProcess.Start();
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
        }
    }
}