﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using dexih.functions;
using dexih.functions.File;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.operations;
using dexih.remote.Operations.Services;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;
using Dexih.Utils.DataType;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace dexih.remote.operations
{
    
    public class RemoteOperations : IRemoteOperations, IDisposable
    {
        private ILogger<RemoteOperations> _logger { get; }
        
        private readonly IManagedTasks _managedTasks;
        private readonly IStreams _streams;
        private readonly ILiveApis _liveApis;
        private readonly RemoteSettings _remoteSettings;
        private readonly ISharedSettings _sharedSettings;
        private readonly IHost _host;

        private readonly HttpClient _httpClient; // used for upload/download to proxy server

        public RemoteOperations(ISharedSettings sharedSettings, ILogger<RemoteOperations> logger, IStreams streams, ILiveApis liveApis, IManagedTasks managedTasks, IHost host)
        {
            _sharedSettings = sharedSettings;
            
            _remoteSettings = _sharedSettings.RemoteSettings;
            _logger = logger;
            _streams = streams;
            _liveApis = liveApis;
            _managedTasks = managedTasks;
            _host = host;

            _httpClient = new HttpClient();
        }

        public void Dispose()
        {
            _httpClient.Dispose();
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
                FilePermissions = _remoteSettings.Permissions.GetFilePermissions()
            };

            return globalSettings;
        }
        

        public TransformSettings GetTransformSettings(DexihHubVariable[] hubHubVariables, IEnumerable<InputParameterBase> inputParameters = null)
        {
            var settings = new TransformSettings()
            {
                HubVariables = hubHubVariables,
                InputParameters = inputParameters?.ToArray(),
                RemoteSettings =  _remoteSettings
            };

            return settings;
        }
        
        public Task<bool> Ping(RemoteMessage message, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }
        
        public  Task<string> Echo(RemoteMessage message, CancellationToken cancellationToken)
        {
            return Task.FromResult(message.Value.ToObject<string>());
        }

        public async Task<RemoteAgentStatus> GetRemoteAgentStatus(RemoteMessage message, CancellationToken cancellationToken)
        {
            try 
            {

                var agentInformation = new RemoteAgentStatus
                {
                    ActiveApis = _liveApis.ActiveApis(),
                    ActiveDatajobs = _managedTasks.GetActiveTasks("Datajob"),
                    ActiveDatalinks = _managedTasks.GetActiveTasks("Datalink"),
                    ActiveDatalinkTests = _managedTasks.GetActiveTasks("DatalinkTest"),
                    PreviousDatajobs = _managedTasks.GetCompletedTasks("Datajob"),
                    PreviousDatalinks = _managedTasks.GetCompletedTasks("Datalink"),
                    PreviousDatalinkTests = _managedTasks.GetCompletedTasks("DatalinkTest"),
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
        public string Encrypt(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
                var value  = message.Value.ToObject<string>();
                var result = EncryptString.Encrypt(value, _remoteSettings.AppSettings.EncryptionKey, _remoteSettings.SystemSettings.EncryptionIterations);
                return result;
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
				var value = message.Value.ToObject<string>();
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

            return false;
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
                            parameter => Dexih.Utils.DataType.Operations.Parse(parameter.DataType, parameter.Rank, testValues[i++])).ToArray<object>();

                    var result = createFunction.function.RunFunction(new FunctionVariables(), inputs, out object[] outputs, cancellationToken);
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
                object testValue = message.Value["testValue"]?.ToObject<object>();

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
                var timer = Stopwatch.StartNew();

                var datalinkKeys = message.Value["datalinkKeys"].ToObject<long[]>();
                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var connectionId = message.Value["connectionId"].ToString();
                
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
                    var dbDatalink = cache.Hub.DexihDatalinks.SingleOrDefault(c => c.Key == datalinkKey);
                    if (dbDatalink == null)
                    {
                        throw new RemoteOperationException($"The datalink with the key {datalinkKey} was not found.");
                    }

                    dbDatalink.UpdateParameters(inputParameters);
                    var datalinkInputs = inputColumns?.Where(c => c.DatalinkKey == dbDatalink.Key).ToArray();
                    var datalinkRun = new DatalinkRun(GetTransformSettings(message.HubVariables, dbDatalink.Parameters), _logger, 0, dbDatalink, cache.Hub, datalinkInputs, transformWriterOptions);
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
                    Reference = reference,
                    OriginatorId = connectionId,
                    Name = $"Datalink: {datalinkRun.Datalink.Name}.",
                    Category = "Datalink",
                    CategoryKey = datalinkRun.Datalink.Key,
                    ReferenceKey = hubKey,
                    ReferenceId = null,
                    ManagedObject = datalinkRun,
                    Triggers = null,
                    FileWatchers = null,
                    DependentReferences = dependencies,
                    ConcurrentTaskAction = parentDataJobRun == null ? EConcurrentTaskAction.Abend : EConcurrentTaskAction.Sequence
                };

                return _managedTasks.Add(task);
//                if (newTask != null)
//                {
//                    return newTask;
//                }
//
//                throw new RemoteOperationException($"Task not successfully created.", null);
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
                                "The datalink could not be cancelled as it is not runnign.");
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
                var references = message.Value.ToObject<string[]>();
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

                var exceptions = new List<Exception>();
                
                foreach (var datalinkTestKey in datalinkTestKeys)
                {
                    var reference = Guid.NewGuid().ToString();
                    
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var datalinkTest = cache.Hub.DexihDatalinkTests.Single(c => c.Key == datalinkTestKey);
                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                        };
                        var datalinkTestRun = new DatalinkTestRun(GetTransformSettings(message.HubVariables), _logger, datalinkTest, cache.Hub, transformWriterOptions);
                        datalinkTestRun.StartMode = EStartMode.RunTests;

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

                var exceptions = new List<Exception>();
                
                foreach (var datalinkTestKey in datalinkTestKeys)
                {
                    var reference = Guid.NewGuid().ToString();
                    
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var datalinkTest = cache.Hub.DexihDatalinkTests.Single(c => c.Key == datalinkTestKey);
                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                        };
                        var datalinkTestRun = new DatalinkTestRun(GetTransformSettings(message.HubVariables), _logger,
                            datalinkTest, cache.Hub, transformWriterOptions) {StartMode = EStartMode.RunSnapshot};
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

        public void RunDatajobs(RemoteMessage message, CancellationToken cancellationToken)
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

                        var dbDatajob = cache.Hub.DexihDatajobs.SingleOrDefault(c => c.Key == datajobKey);
                        if (dbDatajob == null)
                        {
                            throw new Exception($"Datajob with key {datajobKey} was not found");
                        }
                        
                        dbDatajob.UpdateParameters(inputParameters);

                        AddDataJobTask(cache.Hub, GetTransformSettings(message.HubVariables, dbDatajob.Parameters), connectionId, dbDatajob, transformWriterOptions, null, null);
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
			}
            catch (Exception ex)
            {
                _logger.LogError(40, ex, "Error in RunDatajobs: {0}", ex.Message);
                throw new RemoteOperationException("Error running datajobs.  " + ex.Message, ex);
            }
        }

		private void AddDataJobTask(DexihHub dbHub, TransformSettings transformSettings, string connectionId, DexihDatajob dbHubDatajob, TransformWriterOptions transformWriterOptions, IEnumerable<ManagedTaskSchedule> managedTaskSchedules, IEnumerable<ManagedTaskFileWatcher> fileWatchers)
		{
            try
            {
                var datajobRun = new DatajobRun(transformSettings, _logger, dbHubDatajob, dbHub, transformWriterOptions);

//                void DatajobScheduleTask(ManagedTask managedTask, DateTime scheduleTime, CancellationToken ct)
//                {
//                    managedTask.Data = datajobRun.WriterResult;
//                    datajobRun.Schedule(scheduleTime).Wait();
//                }
//
//                Task DatajobCancelScheduledTask(ManagedTask managedTask, CancellationToken ct)
//                {
//                    datajobRun.CancelSchedule(ct);
//                    return Task.CompletedTask;
//                }
//
//                async Task DatajobRunTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
//                {
//                    managedTask.Data = datajobRun.WriterResult;
//
//                    void DatajobProgressUpdate(TransformWriterResult writerResult)
//                    {
//                        progress.Report(writerResult.PercentageComplete, writerResult.RowsTotal, writerResult.IsFinished ? "" : "Running datajob...");
//                    }
//
//
//
//                    datajobRun.ResetEvents();
//
//                    datajobRun.OnDatajobProgressUpdate += DatajobProgressUpdate;
//                    datajobRun.OnDatajobStatusUpdate += DatajobProgressUpdate;
//                    datajobRun.OnDatalinkStart += DatalinkStart;
//
//                    progress.Report(0, 0, "Initializing datajob...");
//
//                    await datajobRun.Initialize(ct);
//
//                    progress.Report(0, 0, "Running datajob...");
//
//                    await datajobRun.Run(ct);
//                }
                
                void DatalinkStart(DatalinkRun datalinkRun)
                {
                    RunDataLink(connectionId, dbHub.HubKey, datalinkRun, datajobRun, null);
                }
                
                datajobRun.OnDatalinkStart += DatalinkStart;

                var newManagedTask = new ManagedTask
                {
                    Reference = Guid.NewGuid().ToString(),
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

        public void ActivateDatajobs(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var datajobKeys = message.Value["datajobKeys"].ToObject<long[]>();
				var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
				var connectionId = message.Value["connectionId"].ToString();

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
                            EncryptionKey = cache.CacheEncryptionKey
                        };

                        var datajob = ActivateDatajob(package);
                        
                        if (datajob.AutoStart && (datajob.DexihTriggers.Count > 0 || datajob.FileWatch) )
                        {
                            var path = _remoteSettings.AutoStartPath();
                            var fileName = $"dexih_datajob_{datajob.Key}.json";
                            var filePath = Path.Combine(path, fileName);
//                            var saveCache = new CacheManager(cache.HubKey, cache.CacheEncryptionKey);
//                            saveCache.AddDatajobs(new [] {datajob.DatajobKey}, cache.Hub);
                            var saveData = Json.JTokenFromObject(package, _remoteSettings.AppSettings.EncryptionKey);
                            
                            File.WriteAllText(filePath, saveData.ToString());
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
                var dbDatajob = autoStart.Hub.DexihDatajobs.SingleOrDefault(c => c.Key == autoStart.Key);
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

                var triggers = new List<ManagedTaskSchedule>();

                foreach (var trigger in dbDatajob.DexihTriggers)
                {
                    var managedTaskSchedule = trigger.CreateManagedTaskSchedule();
                    triggers.Add(managedTaskSchedule);
                }

                List<ManagedTaskFileWatcher> paths = null;

                if (dbDatajob.FileWatch)
                {
                    paths = new List<ManagedTaskFileWatcher>();
                    foreach (var step in dbDatajob.DexihDatalinkSteps)
                    {
                        var datalink = autoStart.Hub.DexihDatalinks.SingleOrDefault(d => d.Key == step.DatalinkKey);
                        if (datalink != null)
                        {
                            var tables = datalink.GetAllSourceTables(autoStart.Hub);

                            foreach (var dbTable in tables.Where(c => c.FileFormatKey != null))
                            {
                                var dbConnection =
                                    autoStart.Hub.DexihConnections.SingleOrDefault(
                                        c => c.Key == dbTable.ConnectionKey);

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
                        dbDatajob, transformWriterOptions, triggers, paths);
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
				var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
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
                            var savedata = Json.JTokenFromObject(package, _remoteSettings.AppSettings.EncryptionKey);
                            
                            File.WriteAllText(filePath, savedata.ToString());
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
                throw new RemoteOperationException("Error deactivating api's.  " + ex.Message, ex);
            }
        }
        
        public async Task<string> CallApi(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var apiKey = message.Value["apiKey"].ToObject<string>();
                var action = message.Value["action"].ToObject<string>();
                var parameters = message.Value["inputParameters"].ToObject<string>();
                var ipAddress = message.Value["ipAddress"].ToObject<string>();
                var proxyUrl = message.Value["proxyUrl"].ToObject<string>();
                
                var data = await _liveApis.Query(apiKey, action, parameters, ipAddress, cancellationToken);
                var byteArray = Encoding.UTF8.GetBytes(data.ToString());
                var stream = new MemoryStream(byteArray);

                var downloadUrl = new DownloadUrl()
                    {Url = proxyUrl, IsEncrypted = true, DownloadUrlType = EDownloadUrlType.Proxy};

                return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "", cancellationToken);

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
                var dbConnection = message.Value.ToObject<DexihConnection>();
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
                var dbConnection = message.Value.ToObject<DexihConnection>();
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
                var dbConnection = message.Value.ToObject<DexihConnection>();
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
                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();

                for(var i = 0; i < dbTables.Count(); i++)
                {
                    var dbTable = dbTables[i];

                    var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey);
                    if (dbConnection == null)
                    {
                        throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                    }

                    var transformSettings = GetTransformSettings(message.HubVariables);
                    var connection = dbConnection.GetConnection(transformSettings);
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
                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();
                var dropTables = message.Value["dropTables"]?.ToObject<bool>() ?? false;

                for (var i = 0; i < dbTables.Count(); i++)
                {
                    var dbTable = dbTables[i];

                    var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey);
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
                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();

                var exceptions = new List<Exception>();

                for(var i = 0; i < dbTables.Count(); i++)
                {
                    try
                    {
                        var dbTable = dbTables[i];

                        var dbConnection = cache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey);
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
        
        public async Task<string> PreviewTable(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var tableKey = message.Value["tableKey"].ToObject<long>();
                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var dbTable = cache.Hub.GetTableFromKey(tableKey);
                var showRejectedData = message.Value["showRejectedData"].ToObject<bool>();
                var selectQuery = message.Value["selectQuery"].ToObject<SelectQuery>();
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
                var parameters =message.Value["inputParameters"]?.ToObject<InputParameters>();
                selectQuery.AddParameters(parameters);

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
                reader = new TransformQuery(reader, selectQuery) {Name = "Preview Query"};
                await reader.Open(0, null, cancellationToken);
                reader.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                _logger.LogInformation("Preview for table: " + dbTable.Name + ".");

                var stream = new StreamJsonCompact(dbTable.Name, reader, 1000, selectQuery?.Rows ?? 100);
                return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_table.json", cancellationToken);

            }
            catch (Exception ex)
            {
                _logger.LogError(150, ex, "Error in PreviewTable: {0}", ex.Message);
                throw;
            }
        }
        
        private async Task<string> StartUploadStream(Func<Stream, Task> uploadAction, DownloadUrl downloadUrl, string format, string fileName, CancellationToken cancellationToken)
        {
            if (downloadUrl.DownloadUrlType == EDownloadUrlType.Proxy)
            {
                // when uploading files through proxy, first issue a "start" on the server to get upload/download urls
                var startResult = await _httpClient.GetAsync($"{downloadUrl.Url}/start/{format}/{fileName}", cancellationToken);

                if (!startResult.IsSuccessStatusCode)
                {
                    throw new RemoteOperationException($"Failed to connect to the proxy server.  Message: {startResult.ReasonPhrase}");
                }

                var jsonResult = JObject.Parse(await startResult.Content.ReadAsStringAsync());

                var upload = jsonResult["UploadUrl"].ToString();
                var download = jsonResult["DownloadUrl"].ToString();
                
                var uploadDataTask = new UploadDataTask(_httpClient, uploadAction, download, upload);
            
                var newManagedTask = new ManagedTask
                {
                    Reference = Guid.NewGuid().ToString(),
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

                return upload;
            }
            else
            {
                var keys = _streams.SetUploadAction("", uploadAction);
                var url = $"{downloadUrl.Url}/upload/{HttpUtility.UrlEncode(keys.Key)}/{HttpUtility.UrlEncode(keys.SecurityKey)}";
                return url;
            }
        }

        public async Task<string> PreviewTransform(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var datalinkTransformKey = message.Value["datalinkTransformKey"]?.ToObject<long>() ?? 0;
                var dbDatalink = message.Value["datalink"].ToObject<DexihDatalink>();
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();

                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    SelectQuery = message.Value["selectQuery"].ToObject<SelectQuery>(),
                };

                var parameters = message.Value["inputParameters"].ToObject<InputParameters>();
                transformWriterOptions.SelectQuery.AddParameters(parameters);

                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, inputColumns,
                    datalinkTransformKey, null, transformWriterOptions);
                var transform = runPlan.sourceTransform;
                var openReturn = await transform.Open(0, null, cancellationToken);
                if (!openReturn)
                {
                    throw new RemoteOperationException("Failed to open the transform.");
                }

                transform.SetCacheMethod(ECacheMethod.DemandCache);
                transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                var stream = new StreamJsonCompact(dbDatalink.Name + " " + transform.Name, transform, 1000, transformWriterOptions.SelectQuery.Rows);
                return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_transform.json", cancellationToken);
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

                var cache =Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var datalinkTransformKey = message.Value["datalinkTransformKey"]?.ToObject<long>() ?? 0;
                var dbDatalink = message.Value["datalink"].ToObject<DexihDatalink>();
                var datalinkTransformItem = message.Value["datalinkTransformItem"].ToObject<DexihDatalinkTransformItem>();

                // get the previous datalink transform, which will be used as input for the import function
                var datalinkTransform = dbDatalink.DexihDatalinkTransforms.Single(c => c.Key == datalinkTransformKey);
                var previousDatalinkTransform = dbDatalink.DexihDatalinkTransforms.OrderBy(c => c.Position).SingleOrDefault(c => c.Position < datalinkTransform.Position);

                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey)
                };
                
                Transform transform;
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                if(previousDatalinkTransform != null) 
                {
                    var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, null, previousDatalinkTransform.Key, null, transformWriterOptions);
                    transform = runPlan.sourceTransform;
                }
                else
                {
                    var sourceTransform = transformOperations.GetSourceTransform(cache.Hub, dbDatalink.SourceDatalinkTable, null, transformWriterOptions);
                    transform = sourceTransform.sourceTransform;
                }

                var openReturn = await transform.Open(0, null, cancellationToken);
                if (!openReturn)
                {
                    throw new RemoteOperationException("Failed to open the transform.");
                }

                transform.SetCacheMethod(ECacheMethod.DemandCache);
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
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in import function mappings: {0}", ex.Message);
                throw new RemoteOperationException(ex.Message, ex);
            }

        }
        
        public async Task<string> PreviewDatalink(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
                var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.Key == datalinkKey);
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
                var parameters = message.Value["inputParameters"].ToObject<InputParameters>();

                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    SelectQuery = message.Value["selectQuery"].ToObject<SelectQuery>()
                };
                
                transformWriterOptions.SelectQuery.AddParameters(parameters);
                
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, inputColumns, null, null, transformWriterOptions);
                var transform = runPlan.sourceTransform;
                var openReturn = await transform.Open(0, null, cancellationToken);
                if (!openReturn)
                {
                    throw new RemoteOperationException("Failed to open the datalink.");
                }

                transform.SetCacheMethod(ECacheMethod.DemandCache);
                transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                var stream = new StreamJsonCompact(dbDatalink.Name, transform, 1000, transformWriterOptions.SelectQuery.Rows);
                return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_datalink.json", cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in PreviewDatalink: {0}", ex.Message);
                throw;
            }

        }

        public async Task<TransformProperties> DatalinkProperties(RemoteMessage message, CancellationToken cancellationToken)
        {
            var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
            var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
            var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.Key == datalinkKey);
            var inputColumns = message.Value["inputColumns"].ToObject<InputColumn[]>();
            var parameters = message.Value["inputParameters"].ToObject<InputParameters>();

            var transformWriterOptions = new TransformWriterOptions()
            {
                PreviewMode = true,
                GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                SelectQuery = message.Value["selectQuery"].ToObject<SelectQuery>()
            };
            
            transformWriterOptions.SelectQuery.AddParameters(parameters);
                
            var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
            var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, inputColumns, null, null, transformWriterOptions);
            var transform = runPlan.sourceTransform;
            var openReturn = await transform.Open(0, null, cancellationToken);

            return transform.GetTransformProperties(true);
        }
        
        
        public async Task<string> GetReaderData(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataDownload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
                var dbDatalink = cache.Hub.DexihDatalinks.Single(c => c.Key == datalinkKey);
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
               
                var transformWriterOptions = new TransformWriterOptions()
                {
                    PreviewMode = true,
                    GlobalSettings = CreateGlobalSettings(cache.CacheEncryptionKey),
                    SelectQuery = message.Value["selectQuery"].ToObject<SelectQuery>()
                };
                
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables, dbDatalink.Parameters));
                var runPlan = transformOperations.CreateRunPlan(cache.Hub, dbDatalink, null, null, null, transformWriterOptions);
                var transform = runPlan.sourceTransform;
                var openReturn = await transform.Open(0, transformWriterOptions.SelectQuery, cancellationToken);
                
                if (!openReturn) 
                {
                    throw new RemoteOperationException("Failed to open the transform.");
                }

                transform.SetCacheMethod(ECacheMethod.DemandCache);
                transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                var stream = new StreamCsv(transform);
                return await _sharedSettings.StartDataStream(stream, downloadUrl, "csv", "reader_data.csv", cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(160, ex, "Error in GetReaderData: {0}", ex.Message);
                throw;
            }

        }

        public async Task<string> PreviewProfile(RemoteMessage message, CancellationToken cancellationToken)
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
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var auditKey = message.Value["auditKey"].ToObject<long>();
                var summaryOnly = message.Value["summaryOnly"].ToObject<bool>();

                var profileTable = new TransformProfile().GetProfileTable(profileTableName);

                var connection = dbConnection.GetConnection(GetTransformSettings(message.HubVariables));

                var existsResult = await connection.TableExists(profileTable, cancellationToken);
                
                if(existsResult)
                {
                    var query = profileTable.DefaultSelectQuery();

                    query.Filters.Add(new Filter(profileTable.GetColumn(TableColumn.EDeltaType.CreateAuditKey), ECompare.IsEqual, auditKey));
                    if (summaryOnly)
                        query.Filters.Add(new Filter(profileTable["IsSummary"], ECompare.IsEqual, true));

                    var reader = connection.GetTransformReader(profileTable);
                    reader = new TransformQuery(reader, query);
                    await reader.Open(0, null, cancellationToken);
                    reader.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                    _logger.LogInformation("Preview for profile results: " + profileTable.Name + ".");
                    var stream = new StreamJsonCompact(profileTable.Name, reader, 1000, query.Rows);

                    return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "preview_table.json", cancellationToken);
                }

                throw new RemoteOperationException("The profile results could not be found on existing managed connections.");
            }
            catch (Exception ex)
            {
                _logger.LogError(170, ex, "Error in PreviewProfile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<string> GetAuditResults(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"],
                    _sharedSettings.SessionEncryptionKey);
                var hubKey = message.HubKey;
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
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

                var content = JsonConvert.SerializeObject(transformWriterResults,
                    new JsonSerializerSettings {ContractResolver = new CamelCasePropertyNamesContractResolver()});
                
                var stream = new MemoryStream();
                var writer = new StreamWriter(stream);
                await writer.WriteAsync(content);
                await writer.FlushAsync();
                stream.Position = 0;
//            var stream = new MemoryStream();
//                var ser = new DataContractJsonSerializer(typeof(List<TransformWriterResult>));
//                ser.WriteObject(stream, transformWriterResults);
//                stream.Position = 0;
                return await _sharedSettings.StartDataStream(stream, downloadUrl, "json", "audit_results.json", cancellationToken);
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
            var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
            var dbTable = message.Value["table"]?.ToObject<DexihTable>();
            var dbConnection =cache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey);
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
                var result = await connectionTable.connection.CreateFilePaths(connectionTable.flatFile);
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
                    var result = await connectionTable.connection.MoveFile(connectionTable.flatFile, file, fromDirectory, toDirectory);
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
                    var result = await connectionTable.connection.DeleteFile(connectionTable.flatFile, path, file);
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

                var fileList = await connectionTable.connection.GetFileList(connectionTable.flatFile, path);

                return fileList;
            }
            catch (Exception ex)
            {
                _logger.LogError(230, ex, "Error in GetFileList: {0}", ex.Message);
                throw;
            }
        }


        public async Task<string> UploadFile(RemoteMessage message, CancellationToken cancellationToken)
        {
             try
            {
                if (!_remoteSettings.Privacy.AllowDataUpload)
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var dbCache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var fileName = message.Value["fileName"].ToObject<string>();
                var path = message.Value["path"].ToObject<EFlatFilePath>();
                var dbTable = dbCache.Hub.DexihTables.FirstOrDefault();
                if (dbTable == null)
                {
                    throw new RemoteOperationException("The table could not be found.");
                }

                var dbConnection = dbCache.Hub.DexihConnections.SingleOrDefault(c => c.Key == dbTable.ConnectionKey);
                if(dbConnection == null)
                {
                    throw new RemoteOperationException("The connection could not be found.");
                }
                
                var transformSettings = GetTransformSettings(message.HubVariables);
                var connection = (ConnectionFlatFile)dbConnection.GetConnection(transformSettings);
                var table = dbTable.GetTable(dbCache.Hub, connection, transformSettings);

                var flatFile = (FlatFile)table;
                

                _logger.LogInformation($"UploadFile for connection: {connection.Name}, Name {flatFile.Name}, FileName {fileName}");


                async Task ProcessTask(Stream stream)
                {
                    try
                    {

//                        if (fileName.EndsWith(".zip"))
//                        {
//                            var memoryStream = new MemoryStream();
//                            await stream.CopyToAsync(memoryStream);
//                            using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read, true))
//                            {
//                                foreach (var entry in archive.Entries)
//                                {
//                                    var saveArchiveFile = await connection.SaveFileStream(flatFile, EFlatFilePath.Incoming, entry.Name, entry.Open());
//                                    if (!saveArchiveFile)
//                                    {
//                                        throw new RemoteOperationException("The save file stream failed.");
//                                    }
//                                }
//                            }
//
//                        }
//                        else if (fileName.EndsWith(".gz"))
//                        {
//                            var newFileName = fileName.Substring(0, fileName.Length - 3);
//
//                            using (var decompressionStream = new GZipStream(stream, CompressionMode.Decompress))
//                            {
//                                var saveArchiveFile = await connection.SaveFileStream(flatFile, EFlatFilePath.Incoming, newFileName, decompressionStream);
//                                if (!saveArchiveFile)
//                                {
//                                    throw new RemoteOperationException("The save file stream failed.");
//                                }
//                            }
//                        }
//                        else
//                        {
                            var saveFile = await connection.SaveFiles(flatFile, path, fileName, stream);
//                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(60, ex, "Error processing uploaded file.  {0}", ex.Message);
                        throw;
                    }
                }

                var url = await StartUploadStream(ProcessTask, downloadUrl, "file", fileName, cancellationToken);
                return url;
            }
            catch (Exception ex)
            {
                _logger.LogError(60, ex, "Error in UploadFiles: {0}", ex.Message);
                throw new RemoteOperationException($"The file upload did not completed.  {ex.Message}", ex);
            }
        }
        
        private async Task<FlatFile> CreateTable(ConnectionFlatFile connection, DexihFileFormat fileFormat, DataType.ETypeCode formatType, string fileName, Stream stream, CancellationToken cancellationToken)
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
                FileRootPath = fileName,
                FormatType = formatType,
                FileConfiguration = fileConfiguration
            };
            

            var fileSample = new StringBuilder();
            var reader = new StreamReader(stream);

            if (formatType == DataType.ETypeCode.Text)
            {
                for (var i = 0; i < 20; i++)
                {
                    fileSample.AppendLine(await reader.ReadLineAsync());

                    if (reader.EndOfStream)
                        break;
                }
            }
            else
            {
                fileSample.AppendLine(await reader.ReadToEndAsync());
            }

            file.FileSample = fileSample.ToString();

            var newFile = (FlatFile) await connection.GetSourceTableInfo(file, cancellationToken);

            stream.Position = 0;
            
            await connection.SaveFileStream(newFile, EFlatFilePath.Incoming, fileName, stream);

            return newFile;
        }
        
        public async Task<(string url, string reference)> BulkUploadFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (!_remoteSettings.Privacy.AllowDataUpload)
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var dbCache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var connectionId = message.Value["connectionId"].ToString();
                var connectionKey = message.Value["connectionKey"].ToObject<long>();
                var fileFormatKey = message.Value["fileFormatKey"].ToObject<long>();
                var formatType = message.Value["formatType"].ToObject<DataType.ETypeCode>();
                var fileName = message.Value["fileName"].ToObject<string>();
                
                var dbConnection = dbCache.Hub.DexihConnections.SingleOrDefault(c => c.Key == connectionKey);
                if (dbConnection == null)
                {
                    throw new RemoteOperationException($"The connection with the key {connectionKey} could not be found.");
                }

                DexihFileFormat dbFileFormat = null;
                if (fileFormatKey > 0)
                {
                    dbFileFormat = dbCache.Hub.DexihFileFormats.SingleOrDefault(c => c.Key == fileFormatKey);
                    if (dbFileFormat == null)
                    {
                        throw new RemoteOperationException(
                            $"The file format with the key {fileFormatKey} could not be found.");
                    }
                }

                var transformSettings = GetTransformSettings(message.HubVariables);
                var transformOperations = new TransformsManager(GetTransformSettings(message.HubVariables));
                var connection = (ConnectionFlatFile) dbConnection.GetConnection(transformSettings);

                var reference = Guid.NewGuid().ToString();

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
                                    flatFiles.Add(await CreateTable(connection, dbFileFormat, formatType, entry.Name, entry.Open(), cancellationToken));
                                }
                            }

                        }
                        else if (fileName.EndsWith(".gz"))
                        {
                            var newFileName = fileName.Substring(0, fileName.Length - 3);

                            using (var decompressionStream = new GZipStream(stream, CompressionMode.Decompress))
                            {
                                flatFiles.Add(await CreateTable(connection, dbFileFormat, formatType, newFileName, decompressionStream, cancellationToken));
                            }
                        }
                        else
                        {
                            flatFiles.Add(await CreateTable(connection, dbFileFormat, formatType, fileName, stream, cancellationToken));
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
                    }
                    
                    var readyMessage = new
                    {
                        message.HubKey,
                        _sharedSettings.InstanceId,
                        _sharedSettings.SecurityToken,
                        ConnectionId = connectionId,
                        reference,
                        tables
                    };
                    
                    var response = await _sharedSettings.PostAsync("Remote/FlatFilesReady", readyMessage, cancellationToken);
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new RemoteOperationException($"The bulk upload files did not complete as the http server returned the response {response.ReasonPhrase}.");
                    }
                }

                var url = await StartUploadStream(ProcessTask, downloadUrl, "file", fileName, cancellationToken);
                return (url, reference);
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
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();

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

                var downloadFilesTask = new DownloadFilesTask(_sharedSettings, message.HubKey, connectionTable.connection, connectionTable.flatFile, path, files, downloadUrl, connectionId, reference);

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

                var cache = Json.JTokenToObject<CacheManager>(message.Value["cache"], _sharedSettings.SessionEncryptionKey);
                var connectionId = message.Value["connectionId"].ToString();
                var downloadObjects = message.Value["downloadObjects"].ToObject<DownloadData.DownloadObject[]>();
                var downloadFormat = message.Value["downloadFormat"].ToObject<DownloadData.EDownloadFormat>();
                var zipFiles = message.Value["zipFiles"].ToObject<bool>();
                var downloadUrl = message.Value["downloadUrl"].ToObject<DownloadUrl>();
                var securityToken = _sharedSettings.SecurityToken;

                var reference = Guid.NewGuid().ToString();
               
                var downloadData = new DownloadData(GetTransformSettings(message.HubVariables), cache, downloadObjects, downloadFormat, zipFiles);
                var downloadDataTask = new DownloadDataTask(_sharedSettings, message.HubKey, downloadData, downloadUrl, connectionId, reference);

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
    }
}