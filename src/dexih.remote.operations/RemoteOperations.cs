using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;
using Dexih.Utils.ManagedTasks;
using Dexih.Utils.MessageHelpers;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    
    public class RemoteOperations
    {
        
       
        private readonly string _permenantEncryptionKey;
        private readonly string _temporaryEncryptionKey;
        private readonly int _encryptionIterations = 1000; //strength of encryption
        private ILogger LoggerMessages { get; }
        private EPrivacyLevel PrivacyLevel { get; }
        private string LocalDataSaveLocation { get; }
        private readonly ManagedTasks _managedTasks;
        private readonly HttpClient _httpClient;
        private string _securityToken;
        private readonly string _url;

        public RemoteOperations(RemoteSettings remoteSettings, string temporaryEncryptionKey, ILogger loggerMessages, HttpClient httpClient, string url)
        {
            _permenantEncryptionKey = remoteSettings.AppSettings.EncryptionKey;
            _temporaryEncryptionKey = temporaryEncryptionKey;
            _encryptionIterations = remoteSettings.SystemSettings.EncryptionIteractions;
            LoggerMessages = loggerMessages;
            PrivacyLevel = remoteSettings.AppSettings.PrivacyLevel;
            LocalDataSaveLocation = remoteSettings.AppSettings.LocalDataSaveLocation;
            _httpClient = httpClient;
            _url = url;

            _managedTasks = new ManagedTasks();
        }

        public IEnumerable<ManagedTask> GetActiveTasks(string category) => _managedTasks.GetActiveTasks(category);
        public IEnumerable<ManagedTask> GetTaskChanges(bool resetTaskChanges) => _managedTasks.GetTaskChanges(resetTaskChanges);
        public int TaskChangesCount() => _managedTasks.TaskChangesCount();
        
        public string SecurityToken
        {
            set => _securityToken = value;
        }
        
        public Task<bool> Ping(RemoteMessage message, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }
        
        public  Task<string> Echo(RemoteMessage message, CancellationToken cancellationToken)
        {
            return Task.FromResult(message.Value.ToObject<string>());
        }

        public Task<RemoteAgentStatus> GetRemoteAgentStatus(RemoteMessage message, CancellationToken cancellationToken)
        {
            try 
            {
                var agentInformation = new RemoteAgentStatus
                {
                    ActiveDatajobs = _managedTasks.GetActiveTasks("Datajob"),
                    ActiveDatalinks = _managedTasks.GetActiveTasks("Datalink"),
                    PreviousDatajobs = _managedTasks.GetCompletedTasks("Datajob"),
                    PreviousDatalinks = _managedTasks.GetCompletedTasks("Datalink")
                };

                return Task.FromResult(agentInformation);

            } catch (Exception ex)
            {
                LoggerMessages.LogError(51, ex, "Error in GetAgentInformation: {0}", ex.Message);
                throw;
            }
        }

        public class RemoteAgentStatus
        {
            public IEnumerable<ManagedTask> ActiveDatajobs { get; set; }
            public IEnumerable<ManagedTask> ActiveDatalinks { get; set; }
            public IEnumerable<ManagedTask> PreviousDatajobs { get; set; }
            public IEnumerable<ManagedTask> PreviousDatalinks { get; set; }
        }
        
        /// <summary>
        /// This encrypts a string using the remoteservers encryption key.  This is used for passwords and connection strings
        /// to ensure the passwords cannot be decrypted without access to the remote server.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<string> Encrypt(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
                var value  = message.Value.ToObject<string>();
                var result = EncryptString.Encrypt(value, _permenantEncryptionKey, _encryptionIterations);
                return Task.FromResult(result);
           }
           catch (Exception ex)
           {
               LoggerMessages.LogError(25, ex, "Error in encrypt string: {0}", ex.Message);
                throw;
           }
        }

		/// <summary>
		/// This decrypts a string using the remoteservers encryption key.  This is used for passwords and connection strings
		/// to ensure the passwords cannot be decrypted without access to the remote server.
		/// </summary>
		/// <returns></returns>
		public Task<string> Decrypt(RemoteMessage message, CancellationToken cancellationToken)
		{
			try
			{
				var value = message.Value.ToObject<string>();
				var result = EncryptString.Decrypt(value, _permenantEncryptionKey, _encryptionIterations);
                return Task.FromResult(result);
            }
            catch (Exception ex)
			{
				LoggerMessages.LogError(25, ex, "Error in encrypt string: {0}", ex.Message);
                throw;
			}
		}



        public async Task<List<object>> TestCustomFunction(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				return await Task.Run(() =>
				{
					var dbDatalinkTransformItem = message.Value["datalinkTransformItem"].ToObject<DexihDatalinkTransformItem>();
					var testValues = message.Value["testValues"].ToObject<object[]>();

					var createFunction = dbDatalinkTransformItem.CreateFunctionMethod(false);

					if (testValues != null)
					{
						var runFunctionResult = createFunction.RunFunction(testValues);
						var outputs = createFunction.Outputs.Select(c => c.Value).ToList();
						outputs.Insert(0, runFunctionResult);
						return outputs;
					}
					return null;

				});
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(25, ex, "Error in TestCustomFunction: {0}", ex.Message);
                throw;
            }
        }

        public async Task<TestColumnValidationResult> TestColumnValidation(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var dbColumnValidation = message.Value["columnValidation"].ToObject<DexihColumnValidation>();
                var dbHub = message.Value["hub"].ToObject<DexihHub>();
                var testValue = message.Value["testValue"].ToObject<object>();

                var validationRun = new ColumnValidationRun(_permenantEncryptionKey, message.HubVariables, dbColumnValidation, dbHub);

                var validateCleanResult = await validationRun.ValidateClean(testValue, cancellationToken);

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
                LoggerMessages.LogError(25, ex, "Error in TestColumnValidation: {0}", ex.Message);
                throw;
            }
        }

        public class TestColumnValidationResult 
        {
            public bool Success { get; set; }
            public string CleanedValue { get; set; }
            public string RejectReason { get; set; }
        }

        public async Task<bool> RunDatalinks(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                return await Task.Run<bool>(() =>
                {

                    var timer = Stopwatch.StartNew();

                    var datalinkKeys = message.Value["datalinkKeys"].ToObject<long[]>();
                    var dbHub = message.Value["hub"].ToObject<DexihHub>();
                    var truncateTarget = message.Value["truncateTarget"]?.ToObject<bool>() ?? false;
                    var resetIncremental = message.Value["resetIncremental"]?.ToObject<bool>() ?? false;
                    var resetIncrementalValue = message.Value["resetIncrementalValue"]?.ToObject<object>();
                    var clientId = message.Value["clientId"].ToString();

                    LoggerMessages.LogInformation(25, "Run datalinks timer1: {0}", timer.Elapsed);

                    foreach (var datalinkKey in datalinkKeys)
                    {
                        var dbDatalink = dbHub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == datalinkKey);
                        if (dbDatalink == null)
                        {
                            throw new RemoteOperationException($"The datalink with the key {datalinkKey} was not found.");
                        }

                        var datalinkRun = new DatalinkRun(_permenantEncryptionKey, message.HubVariables, LoggerMessages, dbDatalink, dbHub, "Datalink", dbDatalink.DatalinkKey, 0, TransformWriterResult.ETriggerMethod.Manual, "Started manually at " + DateTime.Now.ToString(CultureInfo.InvariantCulture), truncateTarget, resetIncremental, resetIncrementalValue, null);

                        var runReturn = RunDataLink(clientId, datalinkRun, null, null);
                    }

                    timer.Stop();
                    LoggerMessages.LogInformation(25, "Run datalinks timer4: {0}", timer.Elapsed);

                    return true;
                });
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(25, ex, "Error in RunDatalinks: {0}", ex.Message);
                throw new RemoteOperationException($"Failed to run datalinks: {ex.Message}", ex);
            }
        }

        private ManagedTask RunDataLink(string clientId, DatalinkRun datalinkRun, DatajobRun parentDataJobRun, string[] dependencies)
        {
            try
            {
                var reference = Guid.NewGuid().ToString();

                // put the download into an action and allow to complete in the scheduler.
                async Task DatalinkRunTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken cancellationToken)
                {
                    progress.Report(0, "Initializing datalink...");

                    await datalinkRun.Initialize(cancellationToken);

                    progress.Report(0, "Compiling datalink...");
                    datalinkRun.Build(cancellationToken);

                    void OnProgressUpdate(TransformWriterResult writerResult)
                    {
                        progress.Report(writerResult.PercentageComplete);
                    }

                    datalinkRun.OnProgressUpdate += OnProgressUpdate;
                    datalinkRun.OnStatusUpdate += OnProgressUpdate;

                    if (parentDataJobRun != null)
                    {
                        datalinkRun.OnProgressUpdate += parentDataJobRun.DatalinkStatus;
                        datalinkRun.OnStatusUpdate += parentDataJobRun.DatalinkStatus;
                    }

                    progress.Report(0, "Running datalink...");
                    await datalinkRun.Run(cancellationToken);

                }

                var newTask = _managedTasks.Add(reference, clientId, $"Datalink: {datalinkRun.Datalink.Name}.", "Datalink", datalinkRun.Datalink.HubKey, datalinkRun.Datalink.DatalinkKey, datalinkRun.WriterResult, DatalinkRunTask, null, null, dependencies);
                if (newTask != null)
                {
                    return newTask;
                }

                throw new RemoteOperationException($"Task not successfully created.", null);
            }
            catch (Exception ex)
            {
                throw new RemoteOperationException($"The datalink {datalinkRun.Datalink.Name} task encountered an error {ex.Message}.", ex);
            }
        }

        public async Task<bool> CancelTasks(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                return await Task.Run<bool>(() =>
                {

                    var references = message.Value.ToObject<string[]>();
                    var hubKey = message.HubKey;

                    foreach (var reference in references)
                    {
                        var managedTask = _managedTasks.GetTask(reference);
                        if (managedTask?.HubKey == hubKey)
                        {
                            managedTask.Cancel();
                        }
                    }

                    return true;
                });
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(30, ex, "Error in CancelTasks: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> RunDatajobs(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var datajobKeys = message.Value["datajobKeys"].ToObject<long[]>();
                var dbHub = message.Value["hub"].ToObject<DexihHub>();
                var truncateTarget = message.Value["truncateTarget"]?.ToObject<bool>()??false;
                var resetIncremental = message.Value["resetIncremental"]?.ToObject<bool>()??false;
                var resetIncrementalValue = message.Value["resetIncrementalValue"]?.ToObject<object>();
                var clientId = message.Value["clientId"].ToString();

                var exceptions = new List<Exception>();

                foreach (var datajobKey in datajobKeys)
                {
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var dbDatajob = dbHub.DexihDatajobs.SingleOrDefault(c => c.DatajobKey == datajobKey);
                        if (dbDatajob == null)
                        {
                            throw new Exception($"Datajob with key {datajobKey} was not found");
                        }

                        var addJobResult = await AddDataJobTask(dbHub, message.HubVariables, clientId, dbDatajob, truncateTarget, resetIncremental, resetIncrementalValue, null, null, TransformWriterResult.ETriggerMethod.Manual, cancellationToken);
                        if (!addJobResult)
                        {
                            throw new Exception($"Failed to start data job {dbDatajob.Name} task.");
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"The datajob failed.  {ex.Message}";
                        LoggerMessages.LogError(error);
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
                LoggerMessages.LogError(40, ex, "Error in RunDatajobs: {0}", ex.Message);
                throw new RemoteOperationException("Error running datajobs.  " + ex.Message, ex);
            }
        }

		private async Task<bool> AddDataJobTask(DexihHub dbHub, IEnumerable<DexihHubVariable> hubVariables, string clientId, DexihDatajob dbDatajob, bool truncateTarget, bool resetIncremental, object resetIncrementalValue, IEnumerable<ManagedTaskSchedule> managedTaskSchedules, IEnumerable<ManagedTaskFileWatcher> fileWatchers, TransformWriterResult.ETriggerMethod triggerMethod, CancellationToken cancellationToken)
		{
            try
            {
                return await Task.Run<bool>(() =>
                {

                    var datajobRun = new DatajobRun(_permenantEncryptionKey, hubVariables, LoggerMessages, dbDatajob, dbHub, truncateTarget, resetIncremental, resetIncrementalValue);

                    async Task DatajobScheduleTask(ManagedTask managedTask, DateTime scheduleTime, CancellationToken ct)
                    {
                        datajobRun.Reset();
                        managedTask.Data = datajobRun.WriterResult;
                        await datajobRun.Schedule(scheduleTime, ct);
                    }

                    async Task DatajobCancelScheduledTask(ManagedTask managedTask, CancellationToken ct)
                    {
                        await datajobRun.CancelSchedule(ct);
                    }

                    async Task DatajobRunTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
                    {
                        managedTask.Data = datajobRun.WriterResult;

                        void OnDatajobProgressUpdate(TransformWriterResult writerResult)
                        {
                            progress.Report(writerResult.PercentageComplete);
                        }

                        void OnDatalinkStart(DatalinkRun datalinkRun)
                        {
                            RunDataLink(clientId, datalinkRun, datajobRun, null);
                        }

                        datajobRun.ResetEvents();

                        datajobRun.OnDatajobProgressUpdate += OnDatajobProgressUpdate;
                        datajobRun.OnDatajobStatusUpdate += OnDatajobProgressUpdate;
                        datajobRun.OnDatalinkStart += OnDatalinkStart;

                        progress.Report(0, "Initializing datajob...");

                        await datajobRun.Initialize(ct);

                        progress.Report(0, "Running datajob...");

                        await datajobRun.Run(triggerMethod, "", ct);
                    }

                    var newManagedTask = new ManagedTask
                    {
                        Reference = Guid.NewGuid().ToString(),
                        OriginatorId = clientId,
                        Name = $"Datajob: {dbDatajob.Name}.",
                        Category = "Datajob",
                        CatagoryKey = dbDatajob.DatajobKey,
                        HubKey = dbDatajob.HubKey,
                        Data = datajobRun.WriterResult,
                        Action = DatajobRunTask,
                        Triggers = managedTaskSchedules,
                        FileWatchers = fileWatchers,
                        ScheduleAction = DatajobScheduleTask,
                        CancelScheduleAction = DatajobCancelScheduledTask
                    };

                    _managedTasks.Add(newManagedTask);

                    return true;
                });
            }
            catch (Exception ex)
            {
                throw new RemoteOperationException($"The datajob {dbDatajob.Name} failed to start.  {ex.Message}", ex);
            }
		}

        public async Task<bool> ActivateDatajobs(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var datajobKeys = message.Value["datajobKeys"].ToObject<long[]>();
				var dbHub = message.Value["hub"].ToObject<DexihHub>();
				var truncateTarget = message.Value["truncateTarget"]?.ToObject<bool>() ?? false;
				var resetIncremental = message.Value["resetIncremental"]?.ToObject<bool>() ?? false;
				var resetIncrementalValue = message.Value["resetIncrementalValue"]?.ToObject<object>();
				var clientId = message.Value["clientId"].ToString();

                var exceptions = new List<Exception>();

                foreach (var datajobKey in datajobKeys)
				{
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var dbDatajob = dbHub.DexihDatajobs.SingleOrDefault(c => c.DatajobKey == datajobKey);
                        if (dbDatajob == null)
                        {
                            throw new Exception($"Datajob with key {datajobKey} was not found");
                        }
                        var triggers = new List<ManagedTaskSchedule>();

                        foreach (var trigger in dbDatajob.DexihTriggers)
                        {
                            var managedTaskSchedule = new ManagedTaskSchedule();
                            trigger.CopyProperties(managedTaskSchedule);
                            triggers.Add(managedTaskSchedule);
                        }

                        List<ManagedTaskFileWatcher> paths = null;

                        if (dbDatajob.FileWatch)
                        {
                            paths = new List<ManagedTaskFileWatcher>();
                            foreach (var step in dbDatajob.DexihDatalinkSteps)
                            {
                                var datalink = dbHub.DexihDatalinks.SingleOrDefault(d => d.DatalinkKey == step.DatalinkKey);
                                if (datalink != null)
                                {
                                    var tables = datalink.GetAllSourceTables(dbHub);

                                    foreach (var dbTable in tables.Where(c => c.FileFormatKey != null))
                                    {
                                        var dbConnection = dbHub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey);

                                        if (dbConnection == null)
                                        {
                                            throw new Exception($"Failed to find the connection with the key {dbTable.ConnectionKey} for table {dbTable.Name}.");
                                        }

                                        var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                                        var table = dbTable.GetTable(dbConnection.DatabaseType.Category, _permenantEncryptionKey, message.HubVariables);

                                        if (table is FlatFile flatFile && connection is ConnectionFlatFile connectionFlatFile)
                                        {
                                            var path = connectionFlatFile.GetFullPath(flatFile, EFlatFilePath.Incoming);
                                            paths.Add(new ManagedTaskFileWatcher(path, flatFile.FileMatchPattern));
                                        }
                                    }
                                }
                            }
                        }

                        var addJobResult = await AddDataJobTask(dbHub, message.HubVariables, clientId, dbDatajob, truncateTarget, resetIncremental, resetIncrementalValue, triggers, paths, TransformWriterResult.ETriggerMethod.Schedule, cancellationToken);
                        if (!addJobResult)
                        {
                            throw new Exception($"Failed to activate data job {dbDatajob.Name} task.");
                        }
                    }
                    catch (Exception ex)
                    {
                        var error = $"Failed to activate datajob.  {ex.Message}";
                        LoggerMessages.LogError(error);
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
                LoggerMessages.LogError(40, ex, "Error in ActivateDatajobs: {0}", ex.Message);
                throw new RemoteOperationException("Error activating datajobs.  " + ex.Message, ex);
            }
        }

        public async Task<bool> CreateDatabase(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
                //Import the datalink metadata.
                var dbConnection = message.Value.ToObject<DexihConnection>();
                var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                await connection.CreateDatabase(dbConnection.DefaultDatabase, cancellationToken);
                
                LoggerMessages.LogInformation("Database created for : {Connection}, with name: {Name}", dbConnection.Name, dbConnection.DefaultDatabase);

                return true;
           }
           catch (Exception ex)
           {
               LoggerMessages.LogError(90, ex, "Error in CreateDatabase: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<string>> RefreshConnection(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                //Import the datalink metadata.
                var dbConnection = message.Value.ToObject<DexihConnection>();
                var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                var connectionTest = await connection.GetDatabaseList(cancellationToken);

                LoggerMessages.LogInformation("Database  connection tested for :{Connection}", dbConnection.Name);

                return connectionTest;

            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(100, ex, "Error in RefreshConnection: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<Table>> DatabaseTableNames(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var dbConnection = message.Value.ToObject<DexihConnection>();
                var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);

                //retrieve the source tables into the cache.
                var tablesResult = await connection.GetTableList(cancellationToken);
                LoggerMessages.LogInformation("Import database table names for :{Connection}", dbConnection.Name);
                return tablesResult;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(110, ex, "Error in DatabaseTableNames: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<DexihTable>> ImportDatabaseTables(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var transformOperations = new TransformsManager(_permenantEncryptionKey, message.HubVariables);
                var dbConnections = message.Value["connections"].ToObject<DexihConnection[]>();
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();

                for(var i = 0; i < dbTables.Count(); i++)
                {
                    var dbTable = dbTables[i];

                    var dbConnection = dbConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey);
                    if (dbConnection == null)
                    {
                        throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                    }
                    
                    var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                    var table = dbTable.GetTable(dbConnection.DatabaseType.Category, _permenantEncryptionKey, message.HubVariables);

                    try
                    {
                        var sourceTable = await connection.GetSourceTableInfo(table, cancellationToken);
                        transformOperations.GetDexihTable(sourceTable, dbTable);
                        dbTable.HubKey = dbConnection.HubKey;
                        dbTable.ConnectionKey = dbConnection.ConnectionKey;
                    }
                    catch (Exception ex)
                    {
                        throw new RemoteOperationException($"Error occurred importing tables: {ex.Message}.", ex);
//                        dbTable.EntityStatus.LastStatus = EntityStatus.EStatus.Error;
//                        dbTable.EntityStatus.Message = ex.Message;
                    }

                    LoggerMessages.LogTrace("Import database table for table {table} and connection {connection} completed.", dbTable.Name, dbConnection.Name);
                }

                LoggerMessages.LogInformation("Import database tables completed");
                return dbTables;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(120, ex, "Error in ImportDatabaseTables: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<DexihTable>> CreateDatabaseTables(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var transformOperations = new TransformsManager(_permenantEncryptionKey, message.HubVariables);
                var dbConnections = message.Value["connections"].ToObject<DexihConnection[]>();
                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();
                var dropTables = message.Value["dropTables"]?.ToObject<bool>() ?? false;

                for (var i = 0; i < dbTables.Count(); i++)
                {
                    var dbTable = dbTables[i];

                    var dbConnection = dbConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey);
                    if (dbConnection == null)
                    {
                        throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                    }

                    var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                    var table = dbTable.GetTable(dbConnection.DatabaseType.Category, _permenantEncryptionKey, message.HubVariables);
                    try
                    {
                        await connection.CreateTable(table, dropTables, cancellationToken);
                        transformOperations.GetDexihTable(table, dbTable);
                        dbTable.HubKey = dbConnection.HubKey;
                        dbTable.ConnectionKey = dbConnection.ConnectionKey;
                    }
                    catch (Exception ex)
                    {
                        throw new RemoteOperationException($"Error occurred creating tables: {ex.Message}.", ex);
                        //                        dbTable.EntityStatus.LastStatus = EntityStatus.EStatus.Error;
                        //                        dbTable.EntityStatus.Message = ex.Message;
                    }

                    LoggerMessages.LogTrace("Create database table for table {table} and connection {connection} completed.", dbTable.Name, dbConnection.Name);
                }

                LoggerMessages.LogInformation("Create database tables completed");
                return dbTables;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(120, ex, "Error in CreateDatabaseTables: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> ClearDatabaseTables(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var dbConnections = message.Value["connections"].ToObject<DexihConnection[]>();
                // var properties = message.Value["properties"]?.ToObject<Dictionary<string, string>>();

                var dbTables = message.Value["tables"].ToObject<List<DexihTable>>();

                var exceptions = new List<Exception>();

                for(var i = 0; i < dbTables.Count(); i++)
                {
                    try
                    {
                        var dbTable = dbTables[i];

                        var dbConnection = dbConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey);
                        if (dbConnection == null)
                        {
                            throw new RemoteOperationException($"The connection for the table {dbTable.Name} could not be found.");
                        }

                        var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                        var table = dbTable.GetTable(dbConnection.DatabaseType.Category, _permenantEncryptionKey, message.HubVariables);
                        await connection.TruncateTable(table, cancellationToken);

                        LoggerMessages.LogTrace("Clear database table for table {table} and connection {connection} completed.", dbTable.Name, dbConnection.Name);
                    } catch(Exception ex)
                    {
                        exceptions.Add(new RemoteOperationException($"Failed to truncate table {dbTables[i].Name}.  {ex.Message}", ex));
                    }
                }

                if(exceptions.Count > 0)
                {
                    throw new AggregateException(exceptions);
                }

                LoggerMessages.LogInformation("Clear database tables completed");
                return true;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(120, ex, "Error in ClearDatabaseTables: {0}", ex.Message);
                throw;
            }
        }
        
        public async Task<Table> PreviewTable(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (PrivacyLevel != EPrivacyLevel.AllowDataDownload && string.IsNullOrEmpty(LocalDataSaveLocation))
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var dbTable = message.Value["table"].ToObject<DexihTable>();
                var dbHub = message.Value["hub"].ToObject<DexihHub>();
                var showRejectedData = message.Value["showRejectedData"].ToObject<bool>();
                var selectQuery = message.Value["selectQuery"].ToObject<SelectQuery>();

                //retrieve the source tables into the cache.
                var transformOperations = new TransformsManager(_permenantEncryptionKey, message.HubVariables);

                var data = await transformOperations.GetPreview(dbTable, dbHub, selectQuery, showRejectedData, cancellationToken);
                LoggerMessages.LogInformation("Preview for table: " + dbTable.Name + ".");

                if (PrivacyLevel == EPrivacyLevel.AllowDataDownload)
                {
                    data.Data.ClearDbNullValues();
                    return data;
                }

                if (!Directory.Exists(LocalDataSaveLocation)) Directory.CreateDirectory(LocalDataSaveLocation);
                var saveFileName = LocalDataSaveLocation + "/dexihpreview_" + Guid.NewGuid() + ".csv";
                File.WriteAllText(saveFileName, data.GetCsv());

                throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.  The preview data have been saved locally to the following file: " + saveFileName, null);
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(150, ex, "Error in PreviewTable: {0}", ex.Message);
                throw;
            }
        }

        public async Task<Table> PreviewTransform(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
               if (PrivacyLevel != EPrivacyLevel.AllowDataDownload && string.IsNullOrEmpty(LocalDataSaveLocation))
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var dbDatalink = message.Value["datalink"].ToObject<DexihDatalink>();
                var dbHub = message.Value["hub"].ToObject<DexihHub>();
                var datalinkTransformKey = message.Value["datalinkTransformKey"]?.ToObject<long>() ?? 0;
                var rows = message.Value["rows"]?.ToObject<long>() ?? long.MaxValue;
                
                var transformOperations = new TransformsManager(_permenantEncryptionKey, message.HubVariables);
                var runPlan = transformOperations.CreateRunPlan(dbHub, dbDatalink, datalinkTransformKey, null, false);
                var transform = runPlan.sourceTransform;
                var openReturn = await transform.Open(0, null, cancellationToken);
                if (!openReturn) {
                    throw new RemoteOperationException("Failed to open the transform.");
                }

                transform.SetCacheMethod(Transform.ECacheMethod.OnDemandCache);
				transform.SetEncryptionMethod(Transform.EEncryptionMethod.MaskSecureFields, "");

                var count = 1;
                //loop through the transform to cache the preview data.
                while ((await transform.ReadAsync(cancellationToken)) && count < rows && cancellationToken.IsCancellationRequested == false)
                {
                    count++;
                }
                transform.Dispose();

               LoggerMessages.LogInformation("Preview for transform in datalink: {1}.", dbDatalink.Name);


               if (PrivacyLevel == EPrivacyLevel.AllowDataDownload)
               {
                    transform.CacheTable.Data.ClearDbNullValues();

                    return transform.CacheTable;
               }
               
               if (!Directory.Exists(LocalDataSaveLocation)) Directory.CreateDirectory(LocalDataSaveLocation);
               var fileName = LocalDataSaveLocation + "/dexihpreview_" + Guid.NewGuid() + ".csv";
               File.WriteAllText(fileName, transform.CacheTable.GetCsv());

               throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.  The preview data have been saved locally to the following file: " + fileName, null);
           }
           catch (Exception ex)
           {
               LoggerMessages.LogError(160, ex, "Error in PreviewTransform: {0}", ex.Message);
				throw new RemoteOperationException(ex.Message, ex);
            }

        }
        
        public async Task<Table> PreviewDatalink(RemoteMessage message, CancellationToken cancellationToken)
        {
           try
           {
                if (PrivacyLevel != EPrivacyLevel.AllowDataDownload && string.IsNullOrEmpty(LocalDataSaveLocation))
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.");
                }

                var dbHub = message.Value["hub"].ToObject<DexihHub>();
                var datalinkKey = message.Value["datalinkKey"].ToObject<long>();
               var selectQuery = message.Value["selectQuery"].ToObject<SelectQuery>();
               var dbDatalink = dbHub.DexihDatalinks.Single(c => c.DatalinkKey == datalinkKey);
               
                var transformOperations = new TransformsManager(_permenantEncryptionKey, message.HubVariables);
                var runPlan = transformOperations.CreateRunPlan(dbHub, dbDatalink, null, null, false, selectQuery);
                var transform = runPlan.sourceTransform;
                var openReturn = await transform.Open(0, null, cancellationToken);
                if (!openReturn) 
                {
                    throw new RemoteOperationException("Failed to open the transform.");
                }

                transform.SetCacheMethod(Transform.ECacheMethod.OnDemandCache);
				transform.SetEncryptionMethod(Transform.EEncryptionMethod.MaskSecureFields, "");

                var count = 1;
                //loop through the transform to cache the preview data.
                while ((await transform.ReadAsync(cancellationToken)) && count < selectQuery.Rows && cancellationToken.IsCancellationRequested == false)
                {
                    count++;
                }
                transform.Dispose();

               LoggerMessages.LogInformation("Preview for transform in datalink: {1}.", dbDatalink.Name);


               if (PrivacyLevel == EPrivacyLevel.AllowDataDownload)
               {
                    transform.CacheTable.Data.ClearDbNullValues();

                    return transform.CacheTable;
               }

               if (!Directory.Exists(LocalDataSaveLocation)) Directory.CreateDirectory(LocalDataSaveLocation);
               var fileName = LocalDataSaveLocation + "/dexihpreview_" + Guid.NewGuid() + ".csv";
               File.WriteAllText(fileName, transform.CacheTable.GetCsv());

               throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.  The preview data have been saved locally to the following file: " + fileName);
           }
           catch (Exception ex)
           {
               LoggerMessages.LogError(160, ex, "Error in PreviewTransform: {0}", ex.Message);
                throw;
            }

        }

        /// <summary>
        /// Called by the Remote controller.  This sends data in chunks back to the remote server.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> GetRemoteData(RemoteMessage message, CancellationToken cancellationToken)
        {
            return await Task.Run<bool>(() =>
            {

                if (PrivacyLevel != EPrivacyLevel.AllowDataDownload && string.IsNullOrEmpty(LocalDataSaveLocation))
                {
                    throw new RemoteSecurityException(
                        "This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var dbCache = Json.JTokenToObject<CacheManager>(message.Value["hub"], _temporaryEncryptionKey);
                var selectQuery =
                    Json.JTokenToObject<SelectQuery>(message.Value["selectQuery"], _temporaryEncryptionKey);

                var datalinkKey = Convert.ToInt64(message.GetParameter("datalinkKey"));
                var continuationToken = message.GetParameter("continuationToken");
                var dbDatalink = dbCache.DexihHub.DexihDatalinks.Single(c => c.DatalinkKey == datalinkKey);

                async Task RemoteExtract(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
                {
                    var remoteDataSender = new RemoteDataSender(_permenantEncryptionKey, message.HubVariables, _httpClient, _url);
                    await remoteDataSender.SendDatalinkData(dbCache.DexihHub, dbDatalink, selectQuery, continuationToken,
                        cancellationToken);
                }

                var getRemoteData = _managedTasks.Add("", $"Remote extract: {dbDatalink.Name}.", "RemoteExtract", dbDatalink.HubKey, dbDatalink.DatalinkKey, null, RemoteExtract, null, null);

                return true;
            }, cancellationToken);
        }

        public async Task<Table> PreviewProfile(RemoteMessage message, CancellationToken cancellationToken) // (long HubKey, string Cache, long DatalinkAuditKey, bool SummaryOnly, CancellationToken cancellationToken)
        {
            try
            {
                if (PrivacyLevel != EPrivacyLevel.AllowDataDownload && string.IsNullOrEmpty(LocalDataSaveLocation))
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                //Import the datalink metadata.
                var dbConnections = message.Value["connections"].ToObject<DexihConnection[]>();
                var profileTableName = message.Value["profileTableName"].ToString();
                var auditKey = message.Value["auditKey"].ToObject<long>();
                var summaryOnly = message.Value["summaryOnly"].ToObject<bool>(); ;

                var profileTable = new TransformProfile().GetProfileTable(profileTableName);

                Table data = null;

                var resultsFound = false;
                foreach (var dbConnection in dbConnections)
                {
                    var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);

                    var existsResult = await connection.TableExists(profileTable, cancellationToken);
                    if(existsResult)
                    {
                        var query = profileTable.DefaultSelectQuery();

                        query.Filters.Add(new Filter(profileTable.GetDeltaColumn(TableColumn.EDeltaType.CreateAuditKey), Filter.ECompare.IsEqual, auditKey));
                        if (summaryOnly)
                            query.Filters.Add(new Filter(new TableColumn("IsSummary"), Filter.ECompare.IsEqual, true));

                        //retrieve the source tables into the cache.
                        data = await connection.GetPreview(profileTable, query, cancellationToken);

                        if(data != null && data.Data.Any())
                        {
                            resultsFound = true;
                            break;
                        }
                    }
                }

                LoggerMessages.LogInformation("Preview of profile data for audit: {0}.", auditKey);

                if (resultsFound)
                {
                    if (PrivacyLevel == EPrivacyLevel.AllowDataDownload)
                    {
                        data.Data.ClearDbNullValues();

                        return data;
                    }
                    else
                    {
                        if (!Directory.Exists(LocalDataSaveLocation)) Directory.CreateDirectory(LocalDataSaveLocation);
                        var fileName = LocalDataSaveLocation + "/dexihpreview_" + Guid.NewGuid() + ".csv";
                        File.WriteAllText(fileName, data.GetCsv());

                        throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data previews.  The preview data have been saved locally to the following file: " + fileName);
                    }
                }
                else
                {
                    throw new RemoteSecurityException("The profile results could not be found on existing managed data points.");
                }
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(170, ex, "Error in PreviewProfile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<List<TransformWriterResult>> GetResults(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var dbConnections = message.Value["connections"].ToObject<DexihConnection[]>();
                var hubKey = message.HubKey;
                var referenceKeys = message.Value["referenceKeys"]?.ToObject<long[]>();
                var auditType = message.Value["auditType"]?.ToObject<string>();
                var auditKey = message.Value["auditKey"]?.ToObject<long>();
                var runStatus = message.Value["runStatus"]?.ToObject<TransformWriterResult.ERunStatus>();
                var previousResult = message.Value["previousResult"]?.ToObject<bool>()??false;
                var previousSuccessResult = message.Value["previousSuccessResult"]?.ToObject<bool>()??false;
                var currentResult = message.Value["currentResult"]?.ToObject<bool>()??false;
                var startTime = message.Value["startTime"]?.ToObject<DateTime>()??null;
                var rows = message.Value["rows"]?.ToObject<int>()??int.MaxValue;
                var parentAuditKey = message.Value["parentAuditKey"]?.ToObject<long>()??null;
                var childItems = message.Value["childItems"]?.ToObject<bool>()??false;

                var transformWriterResults = new List<TransformWriterResult>();

                //_loggerMessages.LogInformation("Preview of datalink results for keys: {keys}", string.Join(",", referenceKeys?.Select(c => c.ToString()).ToArray()));

                foreach (var dbConnection in dbConnections)
                {
                    var connection = dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
                    var writerResults = await connection.GetTransformWriterResults(hubKey, referenceKeys, auditType, auditKey, runStatus, previousResult, previousSuccessResult, currentResult, startTime, rows, parentAuditKey, childItems, cancellationToken);
                    transformWriterResults.AddRange(writerResults);
                }

                return transformWriterResults;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(170, ex, "Error in GetResults: {0}", ex.Message);
                throw;
            }
        }

        private (long hubKey, ConnectionFlatFile connection, FlatFile flatFile) GetFlatFile(RemoteMessage message)
		{
            // Import the datalink metadata.
            var dbHub = message.Value["hub"].ToObject<DexihHub>();
            var dbTable = Json.JTokenToObject<DexihTable>(message.Value["table"], _temporaryEncryptionKey);
            var dbConnection =dbHub.DexihConnections.First();
            var table = dbTable.GetTable(dbConnection.DatabaseType.Category, _permenantEncryptionKey, message.HubVariables);
            var connection = (ConnectionFlatFile)dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);
			return (dbConnection.HubKey, connection, (FlatFile) table);
		}

        public async Task<bool> CreateFilePaths(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
 				var conntectionTable = GetFlatFile(message);
                var result = await conntectionTable.connection.CreateFilePaths(conntectionTable.flatFile);
                return result;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(200, ex, "Error in CreateFilePaths: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> MoveFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var conntectionTable = GetFlatFile(message);

                var fromDirectory = message.Value["fromPath"].ToObject<EFlatFilePath>();
                var toDirectory = message.Value["toPath"].ToObject<EFlatFilePath>();
                var files = message.Value["files"].ToObject<string[]>();

                foreach (var file in files)
                {
                    var result = await conntectionTable.connection.MoveFile(conntectionTable.flatFile, file, fromDirectory, toDirectory);
                    if (!result)
                    {
                        return false;
                    }

                }
                return true;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(210, ex, "Error in MoveFile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> DeleteFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var conntectionTable = GetFlatFile(message);
                var path = message.Value["path"].ToObject<EFlatFilePath>();
                var files = message.Value["files"].ToObject<string[]>();

                foreach(var file in files)
                {
                    var result = await conntectionTable.connection.DeleteFile(conntectionTable.flatFile, path, file);
                    if(!result)
                    {
                        return false;
                    }

                }
                return true;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(220, ex, "Error in DeleteFile: {0}", ex.Message);
                throw;
            }
        }



        public async Task<List<DexihFileProperties>> GetFileList(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
				var conntectionTable = GetFlatFile(message);
                var path = message.Value["path"].ToObject<EFlatFilePath>();

                var fileList = await conntectionTable.connection.GetFileList(conntectionTable.flatFile, path);

                return fileList;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(230, ex, "Error in GetFileList: {0}", ex.Message);
                throw;
            }
        }

        public async Task<bool> SaveFile(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (PrivacyLevel != EPrivacyLevel.AllowDataDownload && PrivacyLevel != EPrivacyLevel.AllowDataUpload  && string.IsNullOrEmpty(LocalDataSaveLocation))
                {
                    throw new RemoteSecurityException("This remote agent's privacy settings does not allow remote data to be accessed.");
                }

                var dbCache = Json.JTokenToObject<CacheManager>(message.Value, _temporaryEncryptionKey);
                var dbConnection = dbCache.DexihHub.DexihConnections.FirstOrDefault();
                if(dbConnection == null)
                {
                    throw new RemoteOperationException("The connection could not be found.");
                }
                var dbTable = dbConnection.DexihTables.FirstOrDefault();
                if (dbTable == null)
                {
                    throw new RemoteOperationException("The table could not be found.");
                }

                var table = dbTable.GetTable(dbConnection.DatabaseType.Category, _permenantEncryptionKey, message.HubVariables);
                var connection = (ConnectionFlatFile)dbConnection.GetConnection(_permenantEncryptionKey, message.HubVariables);

                var flatFile = (FlatFile)table;

                LoggerMessages.LogInformation($"SaveFile for connection: {connection.Name}, FileName {flatFile.Name}.");

				var fileReference = message.GetParameter("FileReference");
				var fileName = message.GetParameter("FileName");

                //progress messages are send and forget as it is not critical that they are received.
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("SecurityToken", _securityToken),
                    new KeyValuePair<string, string>("FileReference", fileReference),
                });

                HttpResponseMessage response = await _httpClient.PostAsync(_url + "Remote/GetFileStream", content, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    if(fileName.EndsWith(".zip"))
                    {
                        using (var archive = new ZipArchive(await response.Content.ReadAsStreamAsync(), ZipArchiveMode.Read, true))
                        {
                            foreach(var entry in archive.Entries)
                            {
                                var saveArchiveFile = await connection.SaveFileStream(flatFile, EFlatFilePath.Incoming, entry.Name, entry.Open());
                                if(!saveArchiveFile)
                                {
                                    throw new RemoteOperationException("The save file stream failed.");
                                }
                            }
                        }

                        return true;
                    } else if (fileName.EndsWith(".gz"))
                    {
                        var newFileName = fileName.Substring(0, fileName.Length - 3);

                        using (var decompressionStream = new GZipStream(await response.Content.ReadAsStreamAsync(), CompressionMode.Decompress))
                        {
                            var saveArchiveFile = await connection.SaveFileStream(flatFile, EFlatFilePath.Incoming, newFileName, decompressionStream);
                            if(!saveArchiveFile)
                            {
                                throw new RemoteOperationException("The save file stream failed.");
                            }
                        }

                        return true;
                    }
                    else
                    {
                        var stream = await response.Content.ReadAsStreamAsync();
                        var saveFile = await connection.SaveFileStream(flatFile, EFlatFilePath.Incoming, fileName, stream);
                        return saveFile;
                    }
                }

                throw new RemoteOperationException(response.ReasonPhrase);
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(240, ex, "Error in SaveFile: {0}", ex.Message);
                throw;
            }
        }

        public async Task<ManagedTask> DownloadFiles(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                var connectionTable = GetFlatFile(message);
                var path = message.Value["path"].ToObject<EFlatFilePath>();
                var files = message.Value["files"].ToObject<string[]>();
                var clientId = message.Value["clientId"].ToString();

                var downloadStream = await connectionTable.connection.DownloadFiles(connectionTable.flatFile, path, files, files.Length > 1);
                var filename = files.Length == 1 ? files[0] : connectionTable.flatFile.Name + "_files.zip";
                var reference = Guid.NewGuid().ToString();

                // put the download into an action and allow to complete in the scheduler.
                async Task DownloadTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
                {
                    //progress messages are send and forget as it is not critical that they are received.
                    using (var content = new MultipartFormDataContent())
                    {
                        content.Add(new StringContent(_securityToken), "SecurityToken");
                        content.Add(new StringContent(clientId), "ClientId");
                        content.Add(new StringContent(reference), "Reference");
                        content.Add(new StringContent(connectionTable.hubKey.ToString()), "HubKey");

                        var data = new StreamContent(downloadStream);
                        content.Add(data, "file", filename);

                        var response = await _httpClient.PostAsync(_url + "Remote/SetFileStream", content, ct);
                        if (!response.IsSuccessStatusCode)
                        {
                            throw new RemoteOperationException($"The file download did not complete as the http server returned the response {response.ReasonPhrase}.");
                        }
                        var returnValue = Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(), _temporaryEncryptionKey);
                        if (!returnValue.Success)
                        {
                            throw new RemoteOperationException($"The file download did not completed.  {returnValue.Message}", returnValue.Exception);
                        }
                    }
                }

                var startdownloadResult = _managedTasks.Add(reference, clientId, $"Download file: {files[0]} from {path}.", "Download", connectionTable.hubKey, 0, null, DownloadTask, null, null, null);
                return startdownloadResult;
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(60, ex, "Error in DownloadFiles: {0}", ex.Message);
                throw;
            }
        }

        public async Task<ManagedTask> DownloadData(RemoteMessage message, CancellationToken cancellationToken)
        {
            try
            {
                return await Task.Run<ManagedTask>(() =>
                {

                    var cache = message.Value["cache"].ToObject<CacheManager>();
                    var clientId = message.Value["clientId"].ToString();
                    var downloadObjects = message.Value["downloadObjects"].ToObject<DownloadData.DownloadObject[]>();
                    var downloadFormat = message.Value["downloadFormat"].ToObject<DownloadData.EDownloadFormat>();
                    var zipFiles = message.Value["zipFiles"].ToObject<bool>();
                    var securityToken = _securityToken;

                    var reference = Guid.NewGuid().ToString();

                    // put the download into an action and allow to complete in the scheduler.
                    async Task DownloadTask(ManagedTask managedTask, ManagedTaskProgress progress, CancellationToken ct)
                    {
                        progress.Report(30, "Running data extract...");
                        var downloadData = new DownloadData(_permenantEncryptionKey, message.HubVariables);
                        var downloadStream = await downloadData.GetStream(cache, downloadObjects, downloadFormat, zipFiles, cancellationToken);
                        var filename = downloadStream.FileName;
                        var stream = downloadStream.Stream;

                        progress.Report(60, "Downloading data...");

                        //progress messages are send and forget as it is not critical that they are received.
                        using (var content = new MultipartFormDataContent())
                        {
                            content.Add(new StringContent(securityToken), "SecurityToken");
                            content.Add(new StringContent(clientId), "ClientId");
                            content.Add(new StringContent(reference), "Reference");
                            content.Add(new StringContent(cache.HubKey.ToString()), "HubKey");

                            var data = new StreamContent(stream);
                            content.Add(data, "file", filename);

                            var response = await _httpClient.PostAsync(_url + "Remote/SetFileStream", content, ct);
                            if (!response.IsSuccessStatusCode)
                            {
                                throw new RemoteOperationException($"The data download did not complete as the http server returned the response {response.ReasonPhrase}.");
                            }
                            var returnValue = Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(), _temporaryEncryptionKey);
                            if (!returnValue.Success)
                            {
                                throw new RemoteOperationException($"The data download did not completed.  {returnValue.Message}", returnValue.Exception);
                            }
                        }
                    }

                    var startdownloadResult = _managedTasks.Add(reference, clientId, $"Download Data File", "Download", cache.HubKey, 0, null, DownloadTask, null, null, null);
                    return startdownloadResult;
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                LoggerMessages.LogError(60, ex, "Error in Downloading data: {0}", ex.Message);
                throw;
            }
        }
    }
}