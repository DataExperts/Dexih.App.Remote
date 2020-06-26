using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.operations.Alerts;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using dexih.transforms.File;
using dexih.transforms.Mapping;
using dexih.transforms.View;
using Dexih.Utils.Crypto;
using Dexih.Utils.DataType;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    
    public class RemoteOperations : IRemoteOperations, IDisposable
    {
        private ILogger<RemoteOperations> _logger { get; }
        
        private readonly IManagedTasks _managedTasks;
        private readonly IMemoryCache _memoryCache;
        private readonly ILiveApis _liveApis;
        private readonly RemoteSettings _remoteSettings;
        private readonly ISharedSettings _sharedSettings;
        private readonly IHost _host;
        private readonly IHttpClientFactory _clientFactory;
        private readonly IAlertQueue _alertQueue;
        
        public RemoteOperations(ISharedSettings sharedSettings, ILogger<RemoteOperations> logger, IMemoryCache memoryCache, ILiveApis liveApis, IManagedTasks managedTasks, IHost host, IHttpClientFactory clientFactory, IAlertQueue alertQueue)
        {
            _sharedSettings = sharedSettings;
            
            _remoteSettings = _sharedSettings.RemoteSettings;
            _logger = logger;
            _memoryCache = memoryCache;
            _liveApis = liveApis;
            _managedTasks = managedTasks;
            _host = host;

            _clientFactory = clientFactory;
            _alertQueue = alertQueue;
        }

        public void Dispose()
        {
        }

        public IEnumerable<ManagedTask> GetActiveTasks(string category) => _managedTasks.GetActiveTasks(category);
        public IEnumerable<ManagedTask> GetTaskChanges(bool resetTaskChanges) => _managedTasks.GetTaskChanges(resetTaskChanges);
        public int TaskChangesCount() => _managedTasks.TaskChangesCount();

        /// <summary>
        /// creates the global variables which get send to the datalink.
        /// </summary>
        /// <param name="cache"></param>
        /// <returns></returns>
        public GlobalSettings CreateGlobalSettings(string hubEncryptionKey)
        {
            string encryptionKey = null;
            if (!string.IsNullOrEmpty(hubEncryptionKey))
            {
                encryptionKey = hubEncryptionKey + _remoteSettings.AppSettings.EncryptionKey;
            }

            var globalSettings = new GlobalSettings()
            {
                EncryptionKey = encryptionKey,
                FilePermissions = _remoteSettings.Permissions.GetFilePermissions(),
                HttpClientFactory = _clientFactory
            };

            return globalSettings;
        }
        

        public TransformSettings GetTransformSettings(DexihHubVariable[] hubHubVariables, IEnumerable<InputParameterBase> inputParameters = null)
        {
            var settings = new TransformSettings()
            {
                HubVariables = hubHubVariables,
                InputParameters = inputParameters?.ToArray(),
                RemoteSettings =  _remoteSettings,
                ClientFactory = _clientFactory
            };

            return settings;
        }
        
        public Task<bool> Ping(RemoteMessage message, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }
        
        public  Task<string> Echo(RemoteMessage message, CancellationToken cancellationToken)
        {
            return Task.FromResult(message.Value["value"].ToObject<string>());
        }

        public async Task<RemoteAgentStatus> GetRemoteAgentStatus(RemoteMessage message, CancellationToken cancellationToken)
        {
            try 
            {

                var agentInformation = new RemoteAgentStatus
                {
                    ActiveApis = _liveApis.ActiveApis().ToArray(),
                    ActiveDatajobs = _managedTasks.GetActiveTasks("Datajob").ToArray(),
                    ActiveDatalinks = _managedTasks.GetActiveTasks("Datalink").ToArray(),
                    ActiveDatalinkTests = _managedTasks.GetActiveTasks("DatalinkTest").ToArray(),
                    PreviousDatajobs = _managedTasks.GetCompletedTasks("Datajob").ToArray(),
                    PreviousDatalinks = _managedTasks.GetCompletedTasks("Datalink").ToArray(),
                    PreviousDatalinkTests = _managedTasks.GetCompletedTasks("DatalinkTest").ToArray(),
                    RemoteLibraries = await _sharedSettings.GetRemoteLibraries(cancellationToken)
                };

                return agentInformation;

            } catch (Exception ex)
            {
                _logger.LogError(51, ex, "Error in GetAgentInformation: {0}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// This encrypts a string using the remoteservers encryption key.  This is used for passwords and connection strings
        /// to ensure the passwords cannot be decrypted without access to the remote server.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<string> Encrypt(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
               // the raw value gets posted separately directly to the remote agent, so avoid sending unencrypted data to central server.
               string value;
               if (message.DownloadUrl.DownloadUrlType != EDownloadUrlType.Proxy)
               {
                   value = await _sharedSettings.GetCacheItem<string>(message.MessageId + "-raw");
               }
               else
               {
                   value = await _sharedSettings.GetAsync<string>(message.DownloadUrl + "/getRaw/" + message.MessageId, cancellationToken);
               }
               
               if (string.IsNullOrEmpty(value))
               {
                   return null;
               }

               return EncryptString.Encrypt(value, _remoteSettings.AppSettings.EncryptionKey, _remoteSettings.SystemSettings.EncryptionIterations);
           }
           catch (Exception ex)
           {
               _logger.LogError(25, ex, "Error in encrypt string: {0}", ex.Message);
                throw;
           }
        }

		/// <summary>
		/// This decrypts a string using the remoteservers encryption key.  This is used for passwords and connection strings
		/// to ensure the passwords cannot be decrypted without access to the remote server.
		/// </summary>
		/// <returns></returns>
		public string Decrypt(RemoteMessage message, CancellationToken cancellationToken)
		{
			try
			{
				var value = message.Value["value"].ToObject<string>();
				var result = EncryptString.Decrypt(value, _remoteSettings.AppSettings.EncryptionKey, _remoteSettings.SystemSettings.EncryptionIterations);
                
                return result;
            }
            catch (Exception ex)
			{
				_logger.LogError(25, ex, "Error in encrypt string: {0}", ex.Message);
                throw;
			}
		}

        public bool ReStart(RemoteMessage message, CancellationToken cancellation)
        {
            var force = message?.Value["force"].ToObject<bool>() ?? true;
            
            if (force || _managedTasks.RunningCount == 0)
            {
                var applicationLifetime = _host.Services.GetService<IApplicationLifetime>();
                applicationLifetime.StopApplication();
                
                return true;
            }

            throw new Exception(
                $"The remote agent {_remoteSettings.AppSettings.Name} cannot be started as there are {_managedTasks.RunningCount} running tasks.");
        }

        public IEnumerable<object> TestCustomFunction(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var dbDatalinkTransformItem = message.Value["datalinkTransformItem"].ToObject<DexihDatalinkTransformItem>();
                var dbHub = new DexihHub() {HubKey = message.HubKey};
                var testValues = message.Value["testValues"]?.ToObject<object[]>();

                var createFunction = dbDatalinkTransformItem.CreateFunctionMethod(dbHub, CreateGlobalSettings(null), false);
                
                if (testValues != null)
                {
                    // var runFunctionResult = createFunction.RunFunction(testValues, outputNames);
                    // var outputs = createFunction.Outputs.Select(c => c.Value).ToList();
                    // outputs.Insert(0, runFunctionResult);

                    var i = 0;

                    var inputs = dbDatalinkTransformItem.DexihFunctionParameters
                        .Where(c => c.Direction == EParameterDirection.Input).Select(
                            parameter => Operations.Parse(parameter.DataType, parameter.Rank, testValues[i++])).ToArray<object>();

                    var result = createFunction.function.RunFunction(new FunctionVariables(), inputs, out var outputs, cancellationToken);
                    return new object[] {result.returnValue}.Concat(outputs);
                }
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(25, ex, "Error in TestCustomFunction: {0}", ex.Message);
                throw;
            }
        }

        public async Task<TestColumnValidationResult> TestColumnValidation(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var columnValidation = message.Value["columnValidation"].ToObject<DexihColumnValidation>();
                var testValue = message.Value["testValue"]?.ToObject<object>();

                var validationRun =
                    new ColumnValidationRun(GetTransformSettings(message.HubVariables), columnValidation, cache.Hub)
                    {
                        DefaultValue = "<default value>"
                    };

                await validationRun.Initialize(cancellationToken);
                var validateCleanResult = validationRun.ValidateClean(testValue);

                var result = new TestColumnValidationResult()
                {
                    Success = validateCleanResult.success,
                    CleanedValue = validateCleanResult.cleanedValue?.ToString(),
                    RejectReason = validateCleanResult.rejectReason
                };

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(25, ex, "Error in TestColumnValidation: {0}", ex.Message);
                throw;
            }
        }

        public class TestColumnValidationResult 
        {
            public bool Success { get; set; }
            public string CleanedValue { get; set; }
            public string RejectReason { get; set; }
        }

        public bool RunDatalinks(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datalinkKeys = message.Value["datalinkKeys"].ToObject<long[]>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
                // var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var connectionId = message.Value["connectionId"].ToString();
                var alertEmails = message.Value["alertEmails"].ToObject<string[]>();
                
                _logger.LogDebug($"Running datalinks.  Progress sent to connection {connectionId}, keys: {message.Value["datalinkKeys"]}");

                var transformWriterOptions = new TransformWriterOptions()
                {
                    TargetAction = message.Value["truncateTarget"]?.ToObject<bool>() ?? false ? TransformWriterOptions.ETargetAction.Truncate : TransformWriterOptions.ETargetAction.None,
                    ResetIncremental = message.Value["resetIncremental"]?.ToObject<bool>() ?? false,
                    ResetIncrementalValue = message.Value["resetIncrementalValue"]?.ToObject<object>(),
                    TriggerMethod = TransformWriterResult.ETriggerMethod.Manual,
                    TriggerInfo = "Started manually at " + DateTime.Now.ToString(CultureInfo.InvariantCulture),
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    PreviewMode = false,
                };

                var inputColumns = message.Value["inputColumns"]?.ToObject<InputColumn[]>();
                var inputParameters = message.Value["inputParameters"]?.ToObject<InputParameters>();
                
                foreach (var datalinkKey in datalinkKeys)
                {
                    var dbDatalink = cache.Hub.DexihDatalinks.SingleOrDefault(c => c.IsValid && c.Key == datalinkKey);
                    if (dbDatalink == null)
                    {
                        throw new RemoteOperationException($"The datalink with the key {datalinkKey} was not found.");
                    }

                    dbDatalink.UpdateParameters(inputParameters);
                    var datalinkInputs = inputColumns?.Where(c => c.DatalinkKey == dbDatalink.Key).ToArray();
                    var datalinkRun = new DatalinkRun(GetTransformSettings(message.HubVariables, dbDatalink.Parameters), _logger, 0, dbDatalink, cache.Hub, datalinkInputs, transformWriterOptions, _alertQueue, alertEmails);
                    var runReturn = RunDataLink(connectionId, cache.HubKey, datalinkRun, null, null);
                }
                
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(25, ex, "Error in RunDatalinks: {0}", ex.Message);
                throw new RemoteOperationException($"Failed to run datalinks: {ex.Message}", ex);
            }
        }

        private  ManagedTask RunDataLink(string connectionId, long hubKey, DatalinkRun datalinkRun, DatajobRun parentDataJobRun, string[] dependencies)
        {
            try
            {
                var reference = Guid.NewGuid().ToString();
                
//                if (parentDataJobRun != null)
//                {
//                    datalinkRun.OnProgressUpdate += parentDataJobRun.DatalinkStatus;
//                    datalinkRun.OnStatusUpdate += parentDataJobRun.DatalinkStatus;
//                }

                // put the download into an action and allow to complete in the scheduler.
//                async Task DatalinkRunTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken cancellationToken)
//                {
//                    // set the data to the writer result, which is used for real-time progress events sent back to the client.
//                    managedTask.Data = datalinkRun.WriterTarget.WriterResult;
//                    
//                    progress.Report(0, 0, "Compiling datalink...");
//                    datalinkRun.Build(cancellationToken);
//
//                    void ProgressUpdate(DatalinkRun datalinkRun2, TransformWriterResult writerResult)
//                    {
//                        if (writerResult.AuditType == "Datalink")
//                        {
//                            progress.Report(writerResult.PercentageComplete, writerResult.RowsTotal + writerResult.RowsReadPrimary,
//                                writerResult.IsFinished ? "" : "Running datalink...");
//                        }
//                    }
//
//                    datalinkRun.OnProgressUpdate += ProgressUpdate;
//                    datalinkRun.OnStatusUpdate += ProgressUpdate;
//
//                    if (parentDataJobRun != null)
//                    {
//                        datalinkRun.OnProgressUpdate += parentDataJobRun.DatalinkStatus;
//                        datalinkRun.OnStatusUpdate += parentDataJobRun.DatalinkStatus;
//                    }
//
//                    progress.Report(0, 0, "Running datalink...");
//                    await datalinkRun.Run(cancellationToken);
//                }
                
                var task = new ManagedTask
                {
                    TaskId = reference,
                    OriginatorId = connectionId,
                    Name = $"Datalink: {datalinkRun.Datalink.Name}.",
                    Category = "Datalink",
                    CategoryKey = datalinkRun.Datalink.Key,
                    ReferenceKey = hubKey,
                    ReferenceId = null,
                    ManagedObject = datalinkRun,
                    Triggers = null,
                    FileWatchers = null,
                    DependentTaskIds = dependencies,
                    ConcurrentTaskAction = parentDataJobRun == null ? EConcurrentTaskAction.Abend : EConcurrentTaskAction.Sequence
                };

                return _managedTasks.Add(task);
            }
            catch (Exception ex)
            {
                throw new RemoteOperationException($"The datalink {datalinkRun.Datalink.Name} task encountered an error {ex.Message}.", ex);
            }
        }
        
        public bool CancelDatalinks(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datalinkKeys = message.Value["datalinkKeys"].ToObject<long[]>();

                var exceptions = new List<Exception>();

                foreach (var datalinkKey in datalinkKeys)
                {
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;
                        var task = _managedTasks.GetTask("Datalink", datalinkKey);
                        if (task == null)
                        {
                            throw new RemoteOperationException(
                                "The datalink could not be cancelled as it is not running.");
                        }

                        task.Cancel();
                    }
                    catch (RemoteOperationException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        var error = $"Failed to cancel datalink.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in Cancel DataLinks: {0}", ex.Message);
                throw new RemoteOperationException("Error Cancel Datalinks.  " + ex.Message, ex);
            }
        }

        public bool CancelDatalinkTests(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datalinkTestKeys = message.Value["datalinkTestKeys"].ToObject<long[]>();

                var exceptions = new List<Exception>();

                foreach (var key in datalinkTestKeys)
                {
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;
                        var task = _managedTasks.GetTask("DatalinkTest", key);
                        task.Cancel();
                    }
                    catch (Exception ex)
                    {
                        var error = $"Failed to cancel datalink test.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in Cancel DataLink tests: {0}", ex.Message);
                throw new RemoteOperationException("Error Cancel Datalink tests.  " + ex.Message, ex);
            }
        }
        
        public bool CancelTasks(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var references = message.Value["value"].ToObject<string[]>();
                var hubKey = message.HubKey;

                foreach (var reference in references)
                {
                    var managedTask = _managedTasks.GetTask(reference);
                    if (managedTask?.ReferenceKey == hubKey)
                    {
                        managedTask.Cancel();
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(30, ex, "Error in CancelTasks: {0}", ex.Message);
                throw;
            }
        }
        
        public bool RunDatalinkTests(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datalinkTestKeys = message.Value["datalinkTestKeys"].ToObject<long[]>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var connectionId = message.Value["connectionId"].ToString();
                var alertEmails = message.Value["alertEmails"].ToObject<string[]>();
                var exceptions = new List<Exception>();
                
                foreach (var datalinkTestKey in datalinkTestKeys)
                {
                    var reference = Guid.NewGuid().ToString();
                    
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var datalinkTest = cache.Hub.DexihDatalinkTests.Single(c => c.IsValid && c.Key == datalinkTestKey);
                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                        };
                        var datalinkTestRun = new DatalinkTestRun(GetTransformSettings(message.HubVariables), _logger,
                            datalinkTest, cache.Hub, transformWriterOptions, _alertQueue, alertEmails) {StartMode = EStartMode.RunTests};

//                        async Task DatalinkTestTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken cancellationToken2)
//                        {
//                            void ProgressUpdate(TransformWriterResult writerResult)
//                            {
//                                progress.Report(writerResult.PercentageComplete, writerResult.Passed + writerResult.Failed, writerResult.IsFinished ? "" : "Running datalink tests...");
//                            }
//
//                            datalinkTestRun.OnProgressUpdate += ProgressUpdate;
//                            
//                            await datalinkTestRun.Initialize("DatalinkTest", cancellationToken);
//                            managedTask.Data = datalinkTestRun.WriterResult;
//
//                            progress.Report(0, 0, $"Running datalink test {datalinkTest.Name}...");
//                            await datalinkTestRun.Run(cancellationToken2);
//                        }
                        
                        var newTask = _managedTasks.Add(reference,  connectionId, $"Datalink Test: {datalinkTest.Name}.", "DatalinkTest", cache.HubKey, null, datalinkTest.Key, datalinkTestRun, null, null, null);
                        if (newTask == null)
                        {
                            throw new RemoteOperationException("Run datalink tests failed, as the task failed to initialize.");
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"The datalink test failed.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if(exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
			}
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in RunDatalinkTests: {0}", ex.Message);
                throw new RemoteOperationException("Error running data link tests.  " + ex.Message, ex);
            }
        }
        
        /// <summary>
        /// Takes a snapshot of the datalink source/target data and uses this as the test data.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="RemoteOperationException"></exception>
        /// <exception cref="AggregateException"></exception>
        public bool RunDatalinkTestSnapshots(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datalinkTestKeys = message.Value["datalinkTestKeys"].ToObject<long[]>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var connectionId = message.Value["connectionId"].ToString();
                var alertEmails = message.Value["alertEmails"].ToObject<string[]>();
                var exceptions = new List<Exception>();
                
                foreach (var datalinkTestKey in datalinkTestKeys)
                {
                    var reference = Guid.NewGuid().ToString();
                    
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var datalinkTest = cache.Hub.DexihDatalinkTests.Single(c => c.IsValid && c.Key == datalinkTestKey);
                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                        };
                        var datalinkTestRun = new DatalinkTestRun(GetTransformSettings(message.HubVariables), _logger,
                            datalinkTest, cache.Hub, transformWriterOptions, _alertQueue, alertEmails) {StartMode = EStartMode.RunSnapshot};
                        //                        ;
//
//                        async Task DatalinkTestSnapshotTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken cancellationToken2)
//                        {
//                            void ProgressUpdate(TransformWriterResult writerResult)
//                            {
//                                progress.Report(writerResult.PercentageComplete, writerResult.Passed + writerResult.Failed, writerResult.IsFinished ? "" : "Running datalink test snapshot...");
//                            }
//
//                            datalinkTestRun.OnProgressUpdate += ProgressUpdate;
//                            datalinkTestRun.OnStatusUpdate += ProgressUpdate;
//
//                            await datalinkTestRun.Initialize("DatalinkTest", cancellationToken);
//                            managedTask.Data = datalinkTestRun.WriterResult;
//                            
//                            progress.Report(0, 0, $"Running datalink test {datalinkTest.Name}...");
//                            await datalinkTestRun.RunSnapshot(cancellationToken2);
//                        }
                        

                        var newTask = _managedTasks.Add(reference, connectionId, $"Datalink Test Snapshot: {datalinkTest.Name}.", "DatalinkTestSnapshot", cache.HubKey, null, datalinkTest.Key, datalinkTestRun, null, null, null);
                        if (newTask == null)
                        {
                            throw new RemoteOperationException("Run datalink test snapshot failed, as the task failed to initialize.");
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"The datalink test failed.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if(exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
			}
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in RunDatalinkTests: {0}", ex.Message);
                throw new RemoteOperationException("Error running data link tests.  " + ex.Message, ex);
            }
        }

        public bool RunDatajobs(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datajobKeys = message.Value["datajobKeys"].ToObject<long[]>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var truncateTarget = message.Value["truncateTarget"]?.ToObject<bool>() ?? false ? TransformWriterOptions.ETargetAction.Truncate : TransformWriterOptions.ETargetAction.None;
                var resetIncremental = message.Value["resetIncremental"]?.ToObject<bool>()??false;
                var resetIncrementalValue = message.Value["resetIncrementalValue"]?.ToObject<object>();
                var connectionId = message.Value["connectionId"].ToString();
                var inputParameters = message.Value["inputParameters"]?.ToObject<InputParameters>();
                var alertEmails = message.Value["alertEmails"].ToObject<string[]>();
                
                var transformWriterOptions = new TransformWriterOptions()
                {
                    TargetAction = truncateTarget,
                    ResetIncremental = resetIncremental,
                    ResetIncrementalValue = resetIncrementalValue,
                    TriggerMethod = TransformWriterResult.ETriggerMethod.Manual,
                    TriggerInfo = "Started manually at " + DateTime.Now.ToString(CultureInfo.InvariantCulture),
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    PreviewMode = false
                };
                
                var exceptions = new List<Exception>();
                
                foreach (var datajobKey in datajobKeys)
                {
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var dbDatajob = cache.Hub.DexihDatajobs.SingleOrDefault(c => c.IsValid && c.Key == datajobKey);
                        if (dbDatajob == null)
                        {
                            throw new Exception($"Datajob with key {datajobKey} was not found");
                        }
                        
                        dbDatajob.UpdateParameters(inputParameters);

                        AddDataJobTask(cache.Hub, GetTransformSettings(message.HubVariables, dbDatajob.Parameters), connectionId, dbDatajob, transformWriterOptions, null, null, alertEmails);
                    }
                    catch (Exception ex)
                    {
                        var error = $"The datajob failed.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if(exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in RunDatajobs: {0}", ex.Message);
                throw new RemoteOperationException("Error running datajobs.  " + ex.Message, ex);
            }
        }

		private void AddDataJobTask(DexihHub dbHub, TransformSettings transformSettings, string connectionId, DexihDatajob dbHubDatajob, TransformWriterOptions transformWriterOptions, IEnumerable<ManagedTaskTrigger> managedTaskSchedules, IEnumerable<ManagedTaskFileWatcher> fileWatchers, string[] alertEmails)
		{
            try
            {
                var datajobRun = new DatajobRun(transformSettings, _logger, dbHubDatajob, dbHub, transformWriterOptions, _alertQueue, alertEmails);
                
                void DatalinkStart(DatalinkRun datalinkRun)
                {
                    RunDataLink(connectionId, dbHub.HubKey, datalinkRun, datajobRun, null);
                }
                
                datajobRun.OnDatalinkStart += DatalinkStart;

                var newManagedTask = new ManagedTask
                {
                    TaskId = Guid.NewGuid().ToString(),
                    OriginatorId = connectionId,
                    Name = $"Datajob: {dbHubDatajob.Name}.",
                    Category = "Datajob",
                    CategoryKey = dbHubDatajob.Key,
                    ReferenceKey = dbHub.HubKey,
                    ManagedObject = datajobRun,
                    Triggers = managedTaskSchedules,
                    FileWatchers = fileWatchers
                };

                _managedTasks.Add(newManagedTask);
            }
            catch (Exception ex)
            {
                throw new RemoteOperationException($"The datajob {dbHubDatajob.Name} failed to start.  {ex.Message}", ex);
            }
		}

        public bool ActivateDatajobs(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var datajobKeys = message.Value["datajobKeys"].ToObject<long[]>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var alertEmails = message.Value["alertEmails"].ToObject<string[]>();
                var exceptions = new List<Exception>();

                foreach (var datajobKey in datajobKeys)
				{
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var package = new AutoStart()
                        {
                            Type = EAutoStartType.Datajob,
                            Key = datajobKey,
                            Hub = cache.Hub,
                            HubVariables = message.HubVariables,
                            EncryptionKey = cache.CacheEncryptionKey,
                            AlertEmails = alertEmails
                        };

                        var datajob = ActivateDatajob(package);
                        
                        if (datajob.AutoStart && (datajob.DexihTriggers.Count > 0 || datajob.FileWatch) )
                        {
                            var path = _remoteSettings.AutoStartPath();
                            var fileName = $"dexih_datajob_{datajob.Key}.json";
                            var filePath = Path.Combine(path, fileName);
//                            var saveCache = new CacheManager(cache.HubKey, cache.CacheEncryptionKey);
//                            saveCache.AddDatajobs(new [] {datajob.DatajobKey}, cache.Hub);
                            package.Encrypt(_remoteSettings.AppSettings.EncryptionKey);
                            var saveData = package.Serialize();
                            
                            File.WriteAllText(filePath, saveData);
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"Failed to activate datajob.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in ActivateDatajobs: {0}", ex.Message);
                throw new RemoteOperationException("Error activating datajobs.  " + ex.Message, ex);
            }
        }

        public DexihDatajob ActivateDatajob(AutoStart autoStart, string connectionId = "none")
        {
            try
            {
                var dbDatajob = autoStart.Hub.DexihDatajobs.SingleOrDefault(c => c.IsValid && c.Key == autoStart.Key);
                if (dbDatajob == null)
                {
                    throw new RemoteOperationException($"Datajob with key {autoStart.Key} was not found");
                }

                _logger.LogInformation("Starting Datajob - {datajob}.", dbDatajob.Name);


                var transformWriterOptions = new TransformWriterOptions()
                {
                    TargetAction = TransformWriterOptions.ETargetAction.None,
                    ResetIncremental = false,
                    ResetIncrementalValue = null,
                    TriggerMethod = TransformWriterResult.ETriggerMethod.Schedule,
                    TriggerInfo = "Schedule activated at " + DateTime.Now.ToString(CultureInfo.InvariantCulture),
                    GlobalSettings = CreateGlobalSettings(autoStart.Hub.EncryptionKey),
                    PreviewMode = false
                };

                var triggers = new List<ManagedTaskTrigger>();

                foreach (var trigger in dbDatajob.DexihTriggers)
                {
                    var managedTaskTrigger = trigger.CreateManagedTaskTrigger();
                    triggers.Add(managedTaskTrigger);
                }

                List<ManagedTaskFileWatcher> paths = null;

                if (dbDatajob.FileWatch)
                {
                    paths = new List<ManagedTaskFileWatcher>();
                    foreach (var step in dbDatajob.DexihDatalinkSteps)
                    {
                        var datalink = autoStart.Hub.DexihDatalinks.SingleOrDefault(d => d.IsValid && d.Key == step.DatalinkKey);
                        if (datalink != null)
                        {
                            var tables = datalink.GetAllSourceTables(autoStart.Hub);

                            foreach (var dbTable in tables.Where(c => c.FileFormatKey != null))
                            {
                                var dbConnection =
                                    autoStart.Hub.DexihConnections.SingleOrDefault(
                                        c => c.IsValid && c.Key == dbTable.ConnectionKey);

                                if (dbConnection == null)
                                {
                                    throw new RemoteOperationException(
                                        $"Failed to start the job {dbDatajob.Name}, due to missing connection with the key {dbTable.ConnectionKey} for table {dbTable.Name}.");
                                }

                                var transformSetting = GetTransformSettings(autoStart.HubVariables, dbDatajob.Parameters);

                                var connection = dbConnection.GetConnection(transformSetting);
                                var table = dbTable.GetTable(autoStart.Hub, connection, step.DexihDatalinkStepColumns,
                                    transformSetting);

                                if (table is FlatFile flatFile && connection is ConnectionFlatFile connectionFlatFile)
                                {
                                    var path = connectionFlatFile.GetFullPath(flatFile, EFlatFilePath.Incoming);
                                    paths.Add(new ManagedTaskFileWatcher(path, flatFile.FileMatchPattern));
                                }
                            }
                        }
                    }
                }

                try
                {
                    AddDataJobTask(autoStart.Hub, GetTransformSettings(autoStart.HubVariables, dbDatajob.Parameters), connectionId,
                        dbDatajob, transformWriterOptions, triggers, paths, autoStart.AlertEmails);
                }
                catch (Exception ex)
                {
                    throw new RemoteOperationException($"Failed to start the job {dbDatajob.Name}.  Error: {ex.Message}", ex);
                }
                
                return dbDatajob;
            }
            catch (Exception ex)
            {
                var message = $"Error activating datajob: {ex.Message}";
                _logger.LogError(40, ex, message);
                throw new RemoteOperationException(message, ex);
            }
        }

        public bool DeactivateDatajobs(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var datajobKeys = message.Value["datajobKeys"].ToObject<long[]>();

                var exceptions = new List<Exception>();

                foreach (var datajobKey in datajobKeys)
				{
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;
                        var task = _managedTasks.GetTask("Datajob", datajobKey);
                        task?.Cancel();
                        
                        var path = _remoteSettings.AutoStartPath();
                        var fileName = $"dexih_datajob_{datajobKey}.json";
                        var filePath = Path.Combine(path, fileName);
                        if(File.Exists(filePath))
                        {
                            File.Delete(filePath);
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"Failed to cancel datajob.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in DeactivateDatajobs: {0}", ex.Message);
                throw new RemoteOperationException("Error DeactivateDatajobs datajobs.  " + ex.Message, ex);
            }
        }

        public bool ActivateApis(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var apiKeys = message.Value["apiKeys"].ToObject<long[]>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
				var connectionId = message.Value["connectionId"].ToString();

                
               
                var exceptions = new List<Exception>();

                foreach (var apiKey in apiKeys)
				{
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var package = new AutoStart()
                        {
                            Type = EAutoStartType.Api,
                            Key = apiKey,
                            Hub = cache.Hub,
                            HubVariables = message.HubVariables,
                        };

                        var result = _liveApis.ActivateApi(package);
                        
                        var dbApi = result.api;
                        if (dbApi.AutoStart)
                        {
                            package.SecurityKey = result.securityKey;
                            var path = _remoteSettings.AutoStartPath();
                            var fileName = $"dexih_api_{dbApi.Key}.json";
                            var filePath = Path.Combine(path, fileName);
//                            var saveCache = new CacheManager(cache.HubKey, cache.CacheEncryptionKey);
//                            saveCache.AddApis(new [] {dbApi.ApiKey}, cache.Hub);
                            package.Encrypt(_remoteSettings.AppSettings.EncryptionKey);
                            var savedata = JsonExtensions.Serialize(package);
                            
                            File.WriteAllText(filePath, savedata);
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"Failed to activate api.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in ActivateApis: {0}", ex.Message);
                throw new RemoteOperationException("Error activating apis.  " + ex.Message, ex);
            }
        }


        
        public bool DeactivateApis(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var hubKey = message.Value["hubKey"].ToObject<long>();
				var apiKeys = message.Value["apiKeys"].ToObject<long[]>();

                var exceptions = new List<Exception>();

                foreach (var apiKey in apiKeys)
				{
                    try
                    {
                        _liveApis.Remove(hubKey, apiKey);
                        
                        var path = _remoteSettings.AutoStartPath();
                        var fileName = $"dexih_api_{apiKey}.json";
                        var filePath = Path.Combine(path, fileName);
                        if(File.Exists(filePath))
                        {
                            File.Delete(filePath);
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"Error removing api with key {apiKey}.  {ex.Message}";
                        _logger.LogError(error);
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                cancellationToken.ThrowIfCancellationRequested();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in DeactivateApis: {0}", ex.Message);
                throw new RemoteOperationException("Error deactivating Api's.  " + ex.Message, ex);
            }
        }
        
        public async Task<Stream> CallApi(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var apiKey = message.Value["apiKey"].ToObject<string>();
                var action = message.Value["action"].ToObject<string>();
                var parameters = message.Value["parameters"].ToObject<string>();
                var ipAddress = message.Value["ipAddress"].ToObject<string>();
                
                var data = await _liveApis.Query(apiKey, action, parameters, ipAddress, cancellationToken);
                var byteArray = Encoding.UTF8.GetBytes(data);
                var stream = new MemoryStream(byteArray);

                return stream;
//                var downloadUrl = new DownloadUrl()
//                    {Url = proxyUrl, IsEncrypted = true, DownloadUrlType = EDownloadUrlType.Proxy};
//
//                return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "", cancellationToken);
//
            }
            catch (Exception ex)
            {
                _logger.LogError(150, ex, "Error in CallApi: {0}", ex.Message);
                throw;
            }
        }
        
        public async Task<bool> CreateDatabase(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
                //Import the datalink metadata.
                var dbConnection = message.Value["value"].ToObject<DexihConnection>();
                var connection = dbConnection.GetConnection(GetTransformSettings(message.HubVariables));
                await connection.CreateDatabase(dbConnection.DefaultDatabase, cancellationToken);
                
                _logger.LogInformation("Database created for : {Connection}, with name: {Name}", dbConnection.Name, dbConnection.DefaultDatabase);

                return true;
           }
           catch (Exception ex)
           {
               _logger.LogError(90, ex, "Error in CreateDatabase: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<string>> RefreshConnection(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                //Import the datalink metadata.
                var dbConnection = message.Value["value"].ToObject<DexihConnection>();
                var connection = dbConnection.GetConnection(GetTransformSettings(message.HubVariables));
                var connectionTest = await connection.GetDatabaseList(cancellationToken);

                _logger.LogInformation("Database  connection tested for :{Connection}", dbConnection.Name);

                return connectionTest;

            }
            catch (Exception ex)
            {
                _logger.LogError(100, ex, "Error in RefreshConnection: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<Table>> DatabaseTableNames(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var dbConnection = message.Value["value"].ToObject<DexihConnection>();
                var connection = dbConnection.GetConnection(GetTransformSettings(message.HubVariables));

                //retrieve the source tables into the cache.
                var tablesResult = await connection.GetTableList(cancellationToken);
                _logger.LogInformation("Import database table names for :{Connection}", dbConnection.Name);
                return tablesResult;
            }
            catch (Exception ex)
            {
                _logger.LogError(110, ex, "Error in DatabaseTableNames: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<DexihTable>> ImportDatabaseTables(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables));
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();

                foreach (var dbTable in dbTables)
                {
                    Connection connection;
                    var transformSettings = GetTransformSettings(message.HubVariables);
                    var table1 = dbTable;
                    var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == table1.ConnectionKey);
                    
                    if (string.IsNullOrEmpty(dbTable.FileSample))
                    {
                        if (dbConnection == null)
                        {
                            throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                        }

                        connection = dbConnection.GetConnection(transformSettings);
                    }
                    else
                    {
                        connection = new ConnectionFlatFileMemory();
                    }

                    var table = dbTable.GetTable(cache.Hub, connection, transformSettings);

                    try
                    {
                        var sourceTable = await connection.GetSourceTableInfo(table, cancellationToken);
                        transformOperations.GetDexihTable(sourceTable, dbTable);
                        // dbTable.HubKey = dbConnection.HubKey;
                        dbTable.ConnectionKey = dbConnection.Key;
                    }
                    catch (Exception ex)
                    {
                        throw new RemoteOperationException($"Error occurred importing tables: {ex.Message}.", ex);
//                        dbTable.EntityStatus.LastStatus = EntityStatus.EStatus.Error;
//                        dbTable.EntityStatus.Message = ex.Message;
                    }

                    _logger.LogTrace("Import database table for table {table} and connection {connection} completed.", dbTable.Name, dbConnection.Name);
                }

                _logger.LogInformation("Import database tables completed");
                return dbTables;
            }
            catch (Exception ex)
            {
                _logger.LogError(120, ex, "Error in ImportDatabaseTables: {0}", ex.Message);
                throw;
            }
        } 

        public async Task<List<DexihTable>> CreateDatabaseTables(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables));
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();
                var dropTables = message.Value["dropTables"]?.ToObject<bool>() ?? false;

                for (var i = 0; i < dbTables.Count; i++)
                {
                    var dbTable = dbTables[i];

                    var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == dbTable.ConnectionKey);
                    if (dbConnection == null)
                    {
                        throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                    }

                    var transformSettings = GetTransformSettings(message.HubVariables);
                    var connection = dbConnection.GetConnection(transformSettings);
                    var table = dbTable.GetTable(cache.Hub, connection, transformSettings);
                    try
                    {
                        await connection.CreateTable(table, dropTables, cancellationToken);
                        transformOperations.GetDexihTable(table, dbTable);
                        // dbTable.HubKey = dbConnection.HubKey;
                        dbTable.ConnectionKey = dbConnection.Key;
                    }
                    catch (Exception ex)
                    {
                        throw new RemoteOperationException($"Error occurred creating tables: {ex.Message}.", ex);
                    }

                    _logger.LogTrace("Create database table for table {table} and connection {connection} completed.", dbTable.Name, dbConnection.Name);
                }

                _logger.LogInformation("Create database tables completed");
                return dbTables;
            }
            catch (Exception ex)
            {
                _logger.LogError(120, ex, "Error in CreateDatabaseTables: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> ClearDatabaseTables(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();

                var exceptions = new List<Exception>();

                for(var i = 0; i < dbTables.Count; i++)
                {
                    try
                    {
                        var dbTable = dbTables[i];

                        var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == dbTable.ConnectionKey);
                        if (dbConnection == null)
                        {
                            throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                        }

                        var transformSettings = GetTransformSettings(message.HubVariables);
                        var connection = dbConnection.GetConnection(transformSettings);
                        var table = dbTable.GetTable(cache.Hub, connection, transformSettings);
                        await connection.TruncateTable(table, cancellationToken);

                        _logger.LogTrace("Clear database table for table {table} and connection {connection} completed.", dbTable.Name, dbConnection.Name);
                    } catch(Exception ex)
                    {
                        exceptions.Add(new RemoteOperationException($"Failed to truncate table {dbTables[i].Name}.  {ex.Message}", ex));
                    }
                }

                if(exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                _logger.LogInformation("Clear database tables completed");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(120, ex, "Error in ClearDatabaseTables: {0}", ex.Message);
                throw;
            }
        }

        private SelectQuery InitializeSelectQuery(SelectQuery selectQuery)
        {
            if (selectQuery == null)
            {
                return new SelectQuery() {Rows = _remoteSettings.SystemSettings.DefaultPreviewRows};
            }
            else
            {
                if (selectQuery.Rows == -1 || selectQuery.Rows > _remoteSettings.SystemSettings.MaxPreviewRows)
                {
                    selectQuery.Rows = _remoteSettings.SystemSettings.MaxPreviewRows;
                }
            }

            return selectQuery;
        }
        
        public Stream PreviewTable(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var tableKey = message.Value["tableKey"].ToObject<long>();
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var dbTable = cache.Hub.GetTableFromKey(tableKey);
                var showRejectedData = message.Value["showRejectedData"].ToObject<bool>();
                var selectQuery = InitializeSelectQuery(message.Value["selectQuery"].ToObject<SelectQuery>());
                var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
                var parameters =message.Value["inputParameters"]?.ToObject<InputParameters>();
                var viewConfig = message.Value["viewConfig"].ToObject<ViewConfig>();
                
                if (parameters?.Count > 0)
                {
                    selectQuery.AddParameters(parameters);
                }

                //retrieve the source tables into the cache.
                var settings = GetTransformSettings(message.HubVariables);

                var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey && c.IsValid);
                if (dbConnection == null)
                {
                    throw new TransformManagerException($"The connection with the key {dbTable.ConnectionKey} was not found.");
                }
                
                var connection = dbConnection.GetConnection(settings);
                var table = showRejectedData ? dbTable.GetRejectedTable(cache.Hub, connection, settings) : dbTable.GetTable(cache.Hub, connection, inputColumns, settings);
                
                var reader = connection.GetTransformReader(table, true);
                reader = new TransformQuery(reader, null) {Name = "Preview Query"};
                // await reader.Open(0, null, cancellationToken);
                reader.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                _logger.LogInformation("Preview for table: " + dbTable.Name + ".");

                return new StreamJsonCompact(dbTable.Name, reader, selectQuery, 1000, viewConfig);
                
                // return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_table.json", cancellationToken);

            }
            catch (Exception ex)
            {
                _logger.LogError(150, ex, "Error in PreviewTable: {0}", ex.Message);
                throw;
            }
        }
        
        private void StartUploadStream(string messageId, Func<Stream, Task> uploadAction, DownloadUrl downloadUrl, string format, string fileName, CancellationToken cancellationToken)
        {
            if (downloadUrl.DownloadUrlType == EDownloadUrlType.Proxy)
            {
                // create url that will download (the client runs the upload)
                var uploadUrl = $"{downloadUrl}/download/{messageId}/{format}/{fileName}";
                
                var uploadDataTask = new UploadDataTask(_clientFactory, uploadAction, uploadUrl);
            
                var newManagedTask = new ManagedTask
                {
                    TaskId = Guid.NewGuid().ToString(),
                    OriginatorId = "none",
                    Name = $"Remote Data",
                    Category = "ProxyUpload",
                    CategoryKey = 0,
                    ReferenceKey = 0,
                    ManagedObject = uploadDataTask,
                    Triggers = null,
                    FileWatchers = null,
                };

                _managedTasks.Add(newManagedTask);
            }
            else
            {
                _memoryCache.Set(messageId, uploadAction, TimeSpan.FromSeconds(300));
            }
        }

        public Stream PreviewTransform(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var datalinkTransformKey = message.Value["datalinkTransformKey"]?.ToObject<long>() ?? 0;
                var dbDatalink = message.Value["datalink"].ToObject<DexihDatalink>();
                var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
                var viewConfig = message.Value["viewConfig"].ToObject<ViewConfig>();
                
                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    SelectQuery = InitializeSelectQuery(InitializeSelectQuery(message.Value["selectQuery"].ToObject<SelectQuery>())),
                };

                var parameters = message.Value["inputParameters"].ToObject<InputParameters>();
                if (parameters?.Count > 0)
                {
                    transformWriterOptions.SelectQuery.AddParameters(parameters);
                    dbDatalink.UpdateParameters(parameters);
                }

                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, inputColumns,
                    datalinkTransformKey, null, transformWriterOptions);
                var transform = runPlan.sourceTransform;
                // transform.SetCacheMethod(ECacheMethod.DemandCache);
                transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                var stream = new StreamJsonCompact(dbDatalink.Name + " " + transform.Name, transform, transformWriterOptions?.SelectQuery, 1000, viewConfig);
                return stream;
                // return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_transform.json", cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in PreviewTransform: {0}", ex.Message);
                throw new RemoteOperationException(ex.Message, ex);
            }
        }
        
        public async Task<string[]> ImportFunctionMappings(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var datalinkTransformKey = message.Value["datalinkTransformKey"]?.ToObject<long>() ?? 0;
                var dbDatalink = message.Value["datalink"].ToObject<DexihDatalink>();
                var datalinkTransformItem = message.Value["datalinkTransformItem"].ToObject<DexihDatalinkTransformItem>();

                // get the previous datalink transform, which will be used as input for the import function
                var datalinkTransform = dbDatalink.DexihDatalinkTransforms.Single(c => c.IsValid && c.Key == datalinkTransformKey);
                var previousDatalinkTransform = dbDatalink.DexihDatalinkTransforms.OrderBy(c => c.Position).LastOrDefault(c => c.IsValid && c.Position < datalinkTransform.Position);

                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey)
                };
                
                Transform transform = null;
                try
                {
                    var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                    if (previousDatalinkTransform != null)
                    {
                        var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, null, previousDatalinkTransform.Key, null, transformWriterOptions);
                        transform = runPlan.sourceTransform;
                    }
                    else
                    {
                        var sourceTransform = transformOperations.GetSourceTransform(cache.Hub, dbDatalink.SourceDatalinkTable, null, transformWriterOptions);
                        transform = sourceTransform.sourceTransform;
                    }

                    //TODO move Open to stream function to avoid timeouts
                    var openReturn = await transform.Open(0, null, cancellationToken);
                    if (!openReturn)
                    {
                        throw new RemoteOperationException("Failed to open the transform.");
                    }

                    // transform.SetCacheMethod(ECacheMethod.DemandCache);
                    transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");
                    var hasRow = await transform.ReadAsync(cancellationToken);
                    if (!hasRow)
                    {
                        throw new RemoteOperationException("Could not import function mappings, as the source contains no data.");
                    }

                    var function = datalinkTransformItem.CreateFunctionMethod(cache.Hub, CreateGlobalSettings(cache.CacheEncryptionKey));

                    var parameterInfos = function.function.ImportMethod.ParameterInfo;
                    var values = new object[parameterInfos.Length];

                    // loop through the import function parameters, and match them to the parameters in the run function.
                    for (var i = 0; i < parameterInfos.Length; i++)
                    {
                        var parameter = function.parameters.Inputs.SingleOrDefault(c => c.Name == parameterInfos[i].Name) ??
                                        function.parameters.ResultInputs.SingleOrDefault(c => c.Name == parameterInfos[i].Name);

                        if (parameter == null)
                        {
                            continue;
                        }
                        if (parameter is ParameterColumn parameterColumn && parameterColumn.Column != null)
                        {
                            values[i] = transform[parameterColumn.Column.Name];
                        }
                        else
                        {
                            values[i] = parameter.Value;
                        }
                    }
                    return function.function.Import(values);
                }
                catch
                {
                    throw;
                } finally
                {
                    transform?.Close();
                }
                
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in import function mappings: {0}", ex.Message);
                throw new RemoteOperationException(ex.Message, ex);
            }

        }
        
        public async Task<Stream> PreviewDatalink(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
                var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.IsValid && c.Key == datalinkKey);
                var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
                var parameters = message.Value["inputParameters"].ToObject<InputParameters>();
                var viewConfig = message.Value["viewConfig"].ToObject<ViewConfig>();
                var previewUpdates = message.Value["previewUpdates"].ToObject<bool>();

                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    SelectQuery = InitializeSelectQuery(message.Value["selectQuery"].ToObject<SelectQuery>())
                };

                if (parameters?.Count > 0)
                {
                    transformWriterOptions.SelectQuery.AddParameters(parameters);
                    dbDatalink.UpdateParameters(parameters);
                }

                var transformSettings = GetTransformSettings(message.HubVariables, dbDatalink.Parameters);
                var transformOperations = new TransformsManager(transformSettings);

                object maxIncremental = null;
                if(previewUpdates && dbDatalink.AuditConnectionKey > 0 && dbDatalink.DexihDatalinkTargets.Count > 0)
                {
                    var auditConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == dbDatalink.AuditConnectionKey);
                    if (auditConnection != null)
                    {
                        var connection = auditConnection.GetConnection(transformSettings);
                        var targetTableKey = dbDatalink.DexihDatalinkTargets.First().TableKey;
                        var previousResult = await connection.GetPreviousSuccessResult(cache.HubKey, targetTableKey, dbDatalink.Key, cancellationToken);
                        if(previousResult != null)
                        {
                            maxIncremental = previousResult.MaxIncrementalValue;
                        }
                    }
                }

               
                var (transform, table) = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, inputColumns, null, maxIncremental, transformWriterOptions);

                if (previewUpdates && dbDatalink.DexihDatalinkTargets.Count > 0)
                {
                    var targetTableKey = dbDatalink.DexihDatalinkTargets.First().TableKey;
                    var dbTargetTable = cache.Hub.GetTableFromKey(targetTableKey);
                    var dbTargetConnection = cache.Hub.DexihConnections.Single(c => c.Key == dbTargetTable.ConnectionKey);
                    var targetConnection = dbTargetConnection.GetConnection(transformSettings);
                    var targetTable = dbTargetTable.GetTable(cache.Hub, targetConnection, transformSettings);

                    var targetReader = targetConnection.GetTransformReader(targetTable);

                    var autoIncrementKey = 0L;
                    
                    // get the last surrogate key it there is one on the table.
                    var autoIncrement = targetTable.GetColumn(EDeltaType.AutoIncrement);
                    if (autoIncrement != null)
                    {
                        autoIncrementKey = await targetConnection.GetMaxValue<long>(targetTable, autoIncrement, cancellationToken);
                    }

                    var maxValidFrom = DateTime.MinValue;
                    if(dbDatalink.UpdateStrategy == EUpdateStrategy.AppendUpdateDeletePreserve || dbDatalink.UpdateStrategy == EUpdateStrategy.AppendUpdatePreserve)
                    {
                        var validFrom = targetTable.GetColumn(EDeltaType.ValidFromDate);
                        maxValidFrom = await targetConnection.GetMaxValue<DateTime>(targetTable, validFrom, cancellationToken);
                    }

                    transform = new TransformDelta(transform, targetReader, dbDatalink.UpdateStrategy,
                        autoIncrementKey,
                        dbDatalink.AddDefaultRow, true, new DeltaValues('C'));
                }

                // transform.SetCacheMethod(ECacheMethod.DemandCache);
                transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                var stream = new StreamJsonCompact(dbDatalink.Name, transform, transformWriterOptions.SelectQuery, 1000, viewConfig);
                return stream;
                // return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_datalink.json", cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in PreviewDatalink: {0}", ex.Message);
                throw;
            }

        }

        public async Task<TransformProperties> DatalinkProperties(RemoteMessage message, CancellationToken cancellationToken)
        {
            var cache = message.Value["cache"].ToObject<CacheManager>();
            var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
            var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.IsValid && c.Key == datalinkKey);
            var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
            var parameters = message.Value["inputParameters"].ToObject<InputParameters>();

            var transformWriterOptions = new TransformWriterOptions()
            {
                PreviewMode = true,
                GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                SelectQuery = InitializeSelectQuery(message.Value["selectQuery"].ToObject<SelectQuery>())
            };

            if (parameters?.Count > 0)
            {
                transformWriterOptions.SelectQuery.AddParameters(parameters);
            }

            var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
            var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, inputColumns, null, null, transformWriterOptions);
            await using var transform = runPlan.sourceTransform;
            
            //TODO move Open to stream function to avoid timeouts
            var openReturn = await transform.Open(0, null, cancellationToken);

            return transform.GetTransformProperties(true);
        }
        
        
        public Stream GetReaderData(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
                var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.IsValid && c.Key == datalinkKey);
               
                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    SelectQuery = message.Value["selectQuery"].ToObject<SelectQuery>()
                };
                
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, null, null, null, transformWriterOptions);
                var transform = runPlan.sourceTransform;
                // var openReturn = await transform.Open(0, transformWriterOptions.SelectQuery, cancellationToken);
                //
                // if (!openReturn) 
                // {
                //     throw new RemoteOperationException("Failed to open the transform.");
                // }

                // transform.SetCacheMethod(ECacheMethod.DemandCache);
                transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                var stream = new StreamCsv(transform, transformWriterOptions.SelectQuery);
                return stream;
                // return await _sharedSettings.StartDataStream(stream, downloadUrl, "csv", "reader_data.csv", cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in GetReaderData: {0}", ex.Message);
                throw;
            }

        }

        public async Task<Stream> PreviewProfile(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                //Import the datalink metadata.
                var dbConnection = message.Value["connection"].ToObject<DexihConnection>();
                var profileTableName = message.Value["profileTableName"].ToString();
                var auditKey = message.Value["auditKey"].ToObject<long>();
                var summaryOnly = message.Value["summaryOnly"].ToObject<bool>();

                var profileTable = new TransformProfile().GetProfileTable(profileTableName);

                var connection = dbConnection.GetConnection(GetTransformSettings(message.HubVariables));

                var existsResult = await connection.TableExists(profileTable, cancellationToken);
                
                if(existsResult)
                {
                    var query = profileTable.DefaultSelectQuery();

                    query.Filters.Add(new Filter(profileTable.GetColumn(EDeltaType.CreateAuditKey), ECompare.IsEqual, auditKey));
                    if (summaryOnly)
                    {
                        query.Filters.Add(new Filter(profileTable["IsSummary"], ECompare.IsEqual, true));
                    }

                    var reader = connection.GetTransformReader(profileTable);
                    reader = new TransformQuery(reader, query);
                    // await reader.Open(0, null, cancellationToken);
                    reader.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                    _logger.LogInformation("Preview for profile results: " + profileTable.Name + ".");
                    var stream = new StreamJsonCompact(profileTable.Name, reader, query, 1000);
                    return stream;
                    // return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_table.json", cancellationToken);
                }

                throw new RemoteOperationException("The profile results could not be found on existing managed connections.");
            }
            catch (Exception ex)
            {
                _logger.LogError(170, ex, "Error in PreviewProfile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<TransformWriterResult>> GetAuditResults(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var cache = message.Value["cache"].ToObject<CacheManager>();
                var hubKey = message.HubKey;
                var referenceKeys = message.Value["referenceKeys"]?.ToObject<long[]>();
                var auditType = message.Value["auditType"]?.ToObject<string>();
                var auditKey = message.Value["auditKey"]?.ToObject<long?>();
                var runStatus = message.Value["runStatus"]?.ToObject<TransformWriterResult.ERunStatus?>();
                var previousResult = message.Value["previousResult"]?.ToObject<bool?>() ?? false;
                var previousSuccessResult = message.Value["previousSuccessResult"]?.ToObject<bool?>() ?? false;
                var currentResult = message.Value["currentResult"]?.ToObject<bool?>() ?? false;
                var startTime = message.Value["startTime"]?.ToObject<DateTime?>();
                var rows = message.Value["rows"]?.ToObject<int?>() ?? int.MaxValue;
                var parentAuditKey = message.Value["parentAuditKey"]?.ToObject<long?>();
                var childItems = message.Value["childItems"]?.ToObject<bool?>() ?? false;

                var transformWriterResults = new List<TransformWriterResult>();

                //_loggerMessages.LogInformation("Preview of datalink results for keys: {keys}", string.Join(",", referenceKeys?.Select(c => c.ToString()).ToArray()));

                foreach (var dbConnection in cache.Hub.DexihConnections)
                {
                    var connection = dbConnection.GetConnection(GetTransformSettings(message.HubVariables));
                    var writerResults = await connection.GetTransformWriterResults(hubKey, dbConnection.Key,
                        referenceKeys, auditType, auditKey, runStatus, previousResult, previousSuccessResult,
                        currentResult, startTime, rows, parentAuditKey, childItems, cancellationToken);
                    transformWriterResults.AddRange(writerResults);
                }

                return transformWriterResults;
            }
            catch (Exception ex)
            {
                _logger.LogError(170, ex, "Error in GetResults: {0}", ex.Message);
                throw;
            }
        }

        private (long hubKey, ConnectionFlatFile connection, FlatFile flatFile) GetFlatFile(RemoteMessage message)
		{
            // Import the datalink metadata.
            var cache = message.Value["cache"].ToObject<CacheManager>();
            var dbTable = message.Value["table"]?.ToObject<DexihTable>();
            var dbConnection =cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == dbTable.ConnectionKey);

            if (dbConnection == null)
            {
                throw new RemoteOperationException($"The connection for the table {dbTable.Name} with connection key {dbTable.ConnectionKey} could not be found.");
            }
            
		    var transformSettings = GetTransformSettings(message.HubVariables);
		    var connection = (ConnectionFlatFile)dbConnection.GetConnection(transformSettings);
            var table = dbTable.GetTable(cache.Hub, connection, transformSettings);
			return (cache.HubKey, connection, (FlatFile) table);
		}

        public async Task<bool> CreateFilePaths(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
 				var connectionTable = GetFlatFile(message);
                var result = await connectionTable.connection.CreateFilePaths(connectionTable.flatFile, cancellationToken);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(200, ex, "Error in CreateFilePaths: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> MoveFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var connectionTable = GetFlatFile(message);

                var fromDirectory = message.Value["fromPath"].ToObject<EFlatFilePath>();
                var toDirectory = message.Value["toPath"].ToObject<EFlatFilePath>();
                var files = message.Value["files"].ToObject<string[]>();

                foreach (var file in files)
                {
                    var result = await connectionTable.connection.MoveFile(connectionTable.flatFile, file, fromDirectory, toDirectory, cancellationToken);
                    if (!result)
                    {
                        return false;
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(210, ex, "Error in MoveFile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> DeleteFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var connectionTable = GetFlatFile(message);
                var path = message.Value["path"].ToObject<EFlatFilePath>();
                var files = message.Value["files"].ToObject<string[]>();

                foreach(var file in files)
                {
                    var result = await connectionTable.connection.DeleteFile(connectionTable.flatFile, path, file, cancellationToken);
                    if(!result)
                    {
                        return false;
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(220, ex, "Error in DeleteFile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<DexihFileProperties>> GetFileList(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var connectionTable = GetFlatFile(message);
                var path = message.Value["path"].ToObject<EFlatFilePath>();

                var fileList = connectionTable.connection.GetFileEnumerator(connectionTable.flatFile, path, null, cancellationToken);
                var files = new List<DexihFileProperties>();
                await foreach (var file in fileList.WithCancellation(cancellationToken))
                {
                    files.Add(file);
                }
                return files;
            }
            catch (Exception ex)
            {
                _logger.LogError(230, ex, "Error in GetFileList: {0}", ex.Message);
                throw;
            }
        }
        
        public void UploadFile(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataUpload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var fileName = message.Value["fileName"].ToObject<string>();
                var path = message.Value["path"].ToObject<EFlatFilePath>();
                var updateStrategy = message.Value["updateStrategy"].ToObject<EUpdateStrategy>();
                var dbTable = cache.Hub.DexihTables.FirstOrDefault();
                if (dbTable == null)
                {
                    throw new RemoteOperationException("The table could not be found.");
                }

                var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == dbTable.ConnectionKey);
                if(dbConnection == null)
                {
                    throw new RemoteOperationException("The connection could not be found.");
                }
                
                var transformSettings = GetTransformSettings(message.HubVariables);
                var connection = dbConnection.GetConnection(transformSettings);
                

                if (connection is ConnectionFlatFile connectionFlatFile)
                {
                    var table = dbTable.GetTable(cache.Hub, connection, transformSettings);
                    var flatFile = (FlatFile) table;

                    _logger.LogInformation(
                        $"UploadFile for connection: {connection.Name}, Name {flatFile.Name}, FileName {fileName}");

                    async Task ProcessTask(Stream stream)
                    {
                        try
                        {
                            var saveFile =
                                await connectionFlatFile.SaveFiles(flatFile, path, fileName, stream, cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(60, ex, "Error processing uploaded file.  {0}", ex.Message);
                            throw;
                        }
                    }

                    StartUploadStream(message.MessageId, ProcessTask, message.DownloadUrl, "file", fileName, cancellationToken);
                }
                else
                {
                    var fileConnection = new ConnectionFlatFileMemory();
                    var table = dbTable.GetTable(cache.Hub, fileConnection, transformSettings);
                    var flatFile = (FlatFile) table;

                    async Task ProcessTask(Stream stream)
                    {
                        await fileConnection.SaveFiles(flatFile, EFlatFilePath.Incoming, fileName, stream, cancellationToken);
                        var fileReader = fileConnection.GetTransformReader(flatFile);
                        
                        var transformWriterResult = new TransformWriterResult()
                        {
                            AuditConnectionKey = 0,
                            AuditType = "FileLoad",
                            HubKey = cache.HubKey,
                            ReferenceKey = dbTable.Key,
                            ParentAuditKey = 0,
                            ReferenceName = dbTable.Name,
                            SourceTableKey = 0,
                            SourceTableName = "",
                        };

                        var transformWriter = new TransformWriterTarget(connection, table, transformWriterResult);
                        await transformWriter.WriteRecordsAsync(fileReader, updateStrategy, TransformWriterTarget.ETransformWriterMethod.Bulk, cancellationToken);
                    }

                    StartUploadStream(message.MessageId, ProcessTask, message.DownloadUrl, "file", fileName, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(60, ex, "Error in UploadFiles: {0}", ex.Message);
                throw new RemoteOperationException($"The file upload did not completed.  {ex.Message}", ex);
            }
        }
        
        private async Task<FlatFile> CreateTable(long hubKey, Connection connection, DexihFileFormat fileFormat, ETypeCode formatType, string fileName, Stream stream, CancellationToken cancellationToken)
        {
            var name = Path.GetFileNameWithoutExtension(fileName);

            var fileConfiguration = fileFormat?.GetFileFormat();

            var file = new FlatFile()
            {
                Name = name,
                LogicalName = name,
                AutoManageFiles = true,
                BaseTableName = name,
                Description = $"File information for {name}.",
                FileRootPath = name,
                FormatType = formatType,
                FileConfiguration = fileConfiguration,
            };
            
            var memoryStream = new MemoryStream();
            var writer = new StreamWriter(memoryStream);

            var fileSample = new StringBuilder();
            var reader = new StreamReader(stream);

            if (formatType == ETypeCode.Text)
            {
                for (var i = 0; i < 1000; i++)
                {
                    var line = await reader.ReadLineAsync();
                    fileSample.AppendLine(line);
                    writer.WriteLine(line);

                    if (reader.EndOfStream)
                        break;
                }
            }
            else
            {
                fileSample.AppendLine(await reader.ReadToEndAsync());
            }

            file.FileSample = fileSample.ToString();
            memoryStream.Position = 0;

            FlatFile newFile;

            if (connection is ConnectionFlatFile connectionFlatFile)
            {
                newFile = (FlatFile) await connectionFlatFile.GetSourceTableInfo(file, cancellationToken);

                // create a contact stream that merges the saved memory stream and what's left in the file stream.
                // this is due to the file stream cannot be reset, and saves memory
                var concatStream = new ConcatenateStream(new[] {memoryStream, stream});

                await connectionFlatFile.SaveFileStream(newFile, EFlatFilePath.Incoming, fileName, concatStream,
                    cancellationToken);
            }
            else
            {
                var connectionFlatFileMemory = new ConnectionFlatFileMemory();
                newFile = (FlatFile) await connectionFlatFileMemory.GetSourceTableInfo(file, cancellationToken);
                
                var concatStream = new ConcatenateStream(new[] {memoryStream, stream});
                
                await connectionFlatFileMemory.SaveFileStream(newFile, EFlatFilePath.Incoming, fileName, concatStream, cancellationToken);
                var fileReader = connectionFlatFileMemory.GetTransformReader(newFile);
                    
                var transformWriterResult = new TransformWriterResult()
                {
                    AuditConnectionKey = 0,
                    AuditType = "FileLoad",
                    HubKey = hubKey,
                    ReferenceKey = 0,
                    ParentAuditKey = 0,
                    ReferenceName = file.Name,
                    SourceTableKey = 0,
                    SourceTableName = "",
                };

                var transformWriter = new TransformWriterTarget(connection, newFile, transformWriterResult);
                await transformWriter.WriteRecordsAsync(fileReader, EUpdateStrategy.Reload, TransformWriterTarget.ETransformWriterMethod.Bulk, cancellationToken);
            }

            return newFile;
        }
        
        public void BulkUploadFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataUpload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var connectionId = message.Value["connectionId"].ToString();
                var connectionKey = message.Value["connectionKey"].ToObject<long>();
                var fileFormatKey = message.Value["fileFormatKey"].ToObject<long>();
                var formatType = message.Value["formatType"].ToObject<ETypeCode>();
                var fileName = message.Value["fileName"].ToObject<string>();
                
                var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == connectionKey);
                if (dbConnection == null)
                {
                    throw new RemoteOperationException($"The connection with the key {connectionKey} could not be found.");
                }

                DexihFileFormat dbFileFormat = null;
                if (fileFormatKey > 0)
                {
                    dbFileFormat = cache.Hub.DexihFileFormats.SingleOrDefault(c => c.IsValid && c.Key == fileFormatKey);
                    if (dbFileFormat == null)
                    {
                        throw new RemoteOperationException(
                            $"The file format with the key {fileFormatKey} could not be found.");
                    }
                }

                var transformSettings = GetTransformSettings(message.HubVariables);
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables));
                var connection = dbConnection.GetConnection(transformSettings);

                var reference = message.MessageId;

                _logger.LogInformation(
                    $"Create files for connection: {connection.Name}, FileName {fileName}");

                async Task ProcessTask(Stream stream)
                {
                    var flatFiles = new List<FlatFile>();

                    try
                    {
                        if (fileName.EndsWith(".zip"))
                        {
                            var memoryStream = new MemoryStream();
                            await stream.CopyToAsync(memoryStream, cancellationToken);
                            using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read, true))
                            {
                                foreach (var entry in archive.Entries)
                                {
                                    flatFiles.Add(await CreateTable(cache.HubKey, connection, dbFileFormat, formatType, entry.Name, entry.Open(), cancellationToken));
                                }
                            }
                        }
                        else if (fileName.EndsWith(".gz"))
                        {
                            var newFileName = fileName.Substring(0, fileName.Length - 3);

                            await using (var decompressionStream = new GZipStream(stream, CompressionMode.Decompress))
                            {
                                flatFiles.Add(await CreateTable(cache.HubKey, connection, dbFileFormat, formatType, newFileName, decompressionStream, cancellationToken));
                            }
                        }
                        else
                        {
                            flatFiles.Add(await CreateTable(cache.HubKey, connection, dbFileFormat, formatType, fileName, stream, cancellationToken));
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(60, ex, "Error processing uploaded file.  {0}", ex.Message);
                        throw;
                    }
                    
                    // convert the table object to dexihTable
                    var tables = flatFiles.Select(c => transformOperations.GetDexihTable(c)).ToArray();
                    foreach (var table in tables)
                    {
                        table.ConnectionKey = dbConnection.Key;
                        table.HubKey = message.HubKey;
                        table.FileFormatKey = dbFileFormat?.Key;
                    }
                    
                    var readyMessage = new FlatFilesReadyMessage()
                    {
                        HubKey = message.HubKey,
                        InstanceId = _sharedSettings.InstanceId,
                        SecurityToken = _sharedSettings.SecurityToken,
                        ConnectionId = connectionId,
                        Reference = reference,
                        Tables = tables
                    };
                    
                    var returnValue = await _sharedSettings.PostAsync<FlatFilesReadyMessage, ReturnValue>("Remote/FlatFilesReady", readyMessage, cancellationToken);
                    if (!returnValue.Success)
                    {
                        throw new RemoteOperationException($"The bulk upload files did not complete as the http server returned the response {returnValue.Message}.", returnValue.Exception);
                    }
                }

                StartUploadStream(message.MessageId, ProcessTask, message.DownloadUrl, "file", fileName, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(60, ex, "Error in UploadFiles: {0}", ex.Message);
                throw new RemoteOperationException($"The file upload did not completed.  {ex.Message}", ex);
            }
        }

        public ManagedTask DownloadFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var connectionTable = GetFlatFile(message);
                var path = message.Value["path"].ToObject<EFlatFilePath>();
                var files = message.Value["files"].ToObject<string[]>();
                var connectionId = message.Value["connectionId"].ToString();

                var reference = Guid.NewGuid().ToString();

//                // put the download into an action and allow to complete in the scheduler.
//                async Task DownloadTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
//                {
//                    progress.Report(50, 1, "Preparing files...");
//
//                    var downloadStream = await connectionTable.connection.DownloadFiles(connectionTable.flatFile, path, files, files.Length > 1);
//                    var filename = files.Length == 1 ? files[0] : connectionTable.flatFile.Name + "_files.zip";
//
//                    progress.Report(100, 2, "Files ready for download...");
//
//                    var result = await StartDataStream(downloadStream, downloadUrl, "file", filename, cancellationToken);
//
//                    var downloadMessage = new
//                    {
//                        _sharedSettings.InstanceId,
//                        _sharedSettings.SecurityToken,
//                        ConnectionId = connectionId,
//                        Reference = reference,
//                        message.HubKey,
//                        Url = result
//                    };
//                    
//                    var response = await _sharedSettings.PostAsync("Remote/DownloadReady", downloadMessage, ct);
//                    if (!response.IsSuccessStatusCode)
//                    {
//                        throw new RemoteOperationException($"The file download did not complete as the http server returned the response {response.ReasonPhrase}.");
//                    }
//
//                    var returnValue = Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(), _sharedSettings.SessionEncryptionKey);
//                    if (!returnValue.Success)
//                    {
//                        throw new RemoteOperationException($"The file download did not completed.  {returnValue.Message}", returnValue.Exception);
//                    }
//                }

                var downloadFilesTask = new DownloadFilesTask(_sharedSettings,message.MessageId,  message.HubKey, connectionTable.connection, connectionTable.flatFile, path, files, message.DownloadUrl, connectionId, reference);

                var startDownloadResult = _managedTasks.Add(reference, connectionId,
                    $"Download file: {files[0]} from {path}.", "Download", connectionTable.hubKey, null, 0, downloadFilesTask, null, null, null);
                return startDownloadResult;
            }
            catch (Exception ex)
            {
                _logger.LogError(60, ex, "Error in DownloadFiles: {0}", ex.Message);
                throw;
            }
        }

        public ManagedTask DownloadData(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var connectionId = message.Value["connectionId"].ToString();
                var downloadObjects = message.Value["downloadObjects"].ToObject<DownloadData.DownloadObject[]>();
                var downloadFormat = message.Value["downloadFormat"].ToObject<DownloadData.EDownloadFormat>();
                var zipFiles = message.Value["zipFiles"].ToObject<bool>();

                var reference = Guid.NewGuid().ToString();
               
                var downloadData = new DownloadData(GetTransformSettings(message.HubVariables), cache, downloadObjects, downloadFormat, zipFiles);
                var downloadDataTask = new DownloadDataTask(_sharedSettings, message.MessageId, message.HubKey, downloadData, message.DownloadUrl, connectionId, reference);

                var startDownloadResult = _managedTasks.Add(reference, connectionId, $"Download Data File", "Download", cache.HubKey, null, 0, downloadDataTask, null, null, null);
                return startDownloadResult;
            }
            catch (Exception ex)
            {
                _logger.LogError(60, ex, "Error in Downloading data: {0}", ex.Message);
                throw;
            }
        }

        public NamingStandards NamingStandards(RemoteMessage message, CancellationToken cancellationToken)
        {
            return _remoteSettings.NamingStandards;
        }
        
        public Stream PreviewListOfValues(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = message.Value["cache"].ToObject<CacheManager>();
                var datalinkKey = message.Value["listOfValuesKey"].ToObject<long>();
                var dbListOfValues = cache.Hub.DexihListOfValues.Single(c => c.IsValid && c.Key == datalinkKey);
                var resetCache = message.Value["resetCache"].ToObject<bool>();

                var settings = GetTransformSettings(message.HubVariables);
                Transform transform = null;

                TableColumn keyColumn;
                TableColumn nameColumn;
                TableColumn descColumn;

                if (resetCache)
                {
                    _memoryCache.Remove(CacheKeys.LookupValues(dbListOfValues.HubKey, dbListOfValues.Key));
                }

                if (dbListOfValues.Cache)
                {
                    if(_memoryCache.TryGetValue<byte[]>(CacheKeys.LookupValues(dbListOfValues.HubKey, dbListOfValues.Key), out var listOfValues))
                    {
                        return new MemoryStream(listOfValues);
                    }
                }

                switch (dbListOfValues.SourceType)
                {
                    case ELOVObjectType.Datalink:
                        var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.IsValid && c.Key == dbListOfValues.SourceDatalinkKey);
                        var columns = dbDatalink.GetOutputTable().DexihDatalinkColumns;
                        keyColumn = columns.SingleOrDefault(c => c.Name == dbListOfValues.KeyColumn).GetTableColumn(null);
                        nameColumn = columns.SingleOrDefault(c => c.Name == dbListOfValues.NameColumn).GetTableColumn(null);
                        descColumn = columns.SingleOrDefault(c => c.Name == dbListOfValues.DescriptionColumn).GetTableColumn(null);
                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            PreviewMode = true,
                            GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                            SelectQuery = dbListOfValues.SelectQuery
                        };
                        var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                        var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, null, null, null, transformWriterOptions);
                        transform = runPlan.sourceTransform;
                        // var openReturn = await transform.Open(0, null, cancellationToken);
                        // if (!openReturn)
                        // {
                        //     throw new RemoteOperationException("Failed to open the datalink.");
                        // }
                        // transform.SetCacheMethod(ECacheMethod.DemandCache);
                        transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");
                        break;
                    case ELOVObjectType.Table:
                        var dbTable = cache.Hub.DexihTables.Single(c => c.IsValid && c.Key == dbListOfValues.SourceTableKey);
                        var columns1 = dbTable.DexihTableColumns;
                        keyColumn = columns1.SingleOrDefault(c => c.Name == dbListOfValues.KeyColumn)?.GetTableColumn(null);
                        nameColumn = columns1.SingleOrDefault(c => c.Name == dbListOfValues.NameColumn)?.GetTableColumn(null);
                        descColumn = columns1.SingleOrDefault(c => c.Name == dbListOfValues.DescriptionColumn)?.GetTableColumn(null);

                        var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey && c.IsValid);
                        if (dbConnection == null)
                        {
                            throw new TransformManagerException($"The connection with the key {dbTable.ConnectionKey} was not found.");
                        }
                
                        var connection = dbConnection.GetConnection(settings);
                        var table = dbTable.GetTable(cache.Hub, connection, (InputColumn[]) null, settings);
                
                        transform = connection.GetTransformReader(table, true);
                        transform = new TransformQuery(transform, dbListOfValues.SelectQuery);
                        
                        break;
                    default:
                        throw new RemoteOperationException($"The source type {dbListOfValues.SourceType} is not supported.");
                }
                
                var mappings = new Mappings(false);

                void AddMapping(TableColumn column, string name)
                {
                    if (column != null)
                    {
                        var outputColumn = column.Copy(false);
                        outputColumn.Name = name;
                        outputColumn.LogicalName = name;
                        outputColumn.DataType = ETypeCode.String;
                        mappings.Add(new MapColumn(column, outputColumn ));
                    }
                }

                AddMapping(keyColumn, "key");
                AddMapping(nameColumn, "name");
                AddMapping(descColumn, "description");
                
                var lovTransform = new TransformMapping(transform, mappings);
                // await lovTransform.Open(cancellationToken);

                var stream = new StreamJson(lovTransform,  dbListOfValues.SelectQuery?.Rows ?? -1);

                if (dbListOfValues.Cache)
                {
                    var key = CacheKeys.LookupValues(dbListOfValues.HubKey, dbListOfValues.Key);
                    return new CacheStream(stream, _memoryCache, key, dbListOfValues.CacheSeconds);
                }
                else
                {
                    return stream;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in PreviewListOfValues: {0}", ex.Message);
                throw;
            }

        }
    }
}