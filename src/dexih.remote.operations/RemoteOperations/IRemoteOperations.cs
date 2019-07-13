using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.ManagedTasks;

namespace dexih.remote.operations
{
    public interface IRemoteOperations
    {

        IEnumerable<ManagedTask> GetActiveTasks(string category);
        IEnumerable<ManagedTask> GetTaskChanges(bool resetTaskChanges);
        int TaskChangesCount();

        /// <summary>
        /// creates the global variables which get send to the datalink.
        /// </summary>
        /// <param name="cache"></param>
        /// <returns></returns>
        GlobalVariables CreateGlobalVariables(string hubEncryptionKey);

        TransformSettings GetTransformSettings(DexihHubVariable[] hubHubVariables);
        Task<bool> Ping(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> Echo(RemoteMessage message, CancellationToken cancellationToken);
        Task<RemoteAgentStatus> GetRemoteAgentStatus(RemoteMessage message, CancellationToken cancellationToken);

        /// <summary>
        /// This encrypts a string using the remoteservers encryption key.  This is used for passwords and connection strings
        /// to ensure the passwords cannot be decrypted without access to the remote server.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        string Encrypt(RemoteMessage message, CancellationToken cancellationToken);

        /// <summary>
        /// This decrypts a string using the remoteservers encryption key.  This is used for passwords and connection strings
        /// to ensure the passwords cannot be decrypted without access to the remote server.
        /// </summary>
        /// <returns></returns>
        string Decrypt(RemoteMessage message, CancellationToken cancellationToken);

        bool ReStart(RemoteMessage message, CancellationToken cancellation);
        IEnumerable<object> TestCustomFunction(RemoteMessage message, CancellationToken cancellationToken);
        Task<RemoteOperations.TestColumnValidationResult> TestColumnValidation(RemoteMessage message, CancellationToken cancellationToken);
        bool RunDatalinks(RemoteMessage message, CancellationToken cancellationToken);
        bool CancelDatalinks(RemoteMessage message, CancellationToken cancellationToken);
        bool CancelDatalinkTests(RemoteMessage message, CancellationToken cancellationToken);
        bool CancelTasks(RemoteMessage message, CancellationToken cancellationToken);
        bool RunDatalinkTests(RemoteMessage message, CancellationToken cancellationToken);

        /// <summary>
        /// Takes a snapshot of the datalink source/target data and uses this as the test data.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="RemoteOperationException"></exception>
        /// <exception cref="AggregateException"></exception>
        bool RunDatalinkTestSnapshots(RemoteMessage message, CancellationToken cancellationToken);

        Task RunDatajobs(RemoteMessage message, CancellationToken cancellationToken);
        Task ActivateDatajobs(RemoteMessage message, CancellationToken cancellationToken);
        Task<DexihDatajob> ActivateDatajob(AutoStart autoStart, string connectionId = "none");
        bool DeactivateDatajobs(RemoteMessage message, CancellationToken cancellationToken);
        bool ActivateApis(RemoteMessage message, CancellationToken cancellationToken);
        bool DeactivateApis(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> CallApi(RemoteMessage message, CancellationToken cancellationToken);
        Task<bool> CreateDatabase(RemoteMessage message, CancellationToken cancellationToken);
        Task<List<string>> RefreshConnection(RemoteMessage message, CancellationToken cancellationToken);
        Task<List<Table>> DatabaseTableNames(RemoteMessage message, CancellationToken cancellationToken);
        Task<List<DexihTable>> ImportDatabaseTables(RemoteMessage message, CancellationToken cancellationToken);
        Task<List<DexihTable>> CreateDatabaseTables(RemoteMessage message, CancellationToken cancellationToken);
        Task<bool> ClearDatabaseTables(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> PreviewTable(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> PreviewTransform(RemoteMessage message, CancellationToken cancellationToken);
        Task<string[]> ImportFunctionMappings(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> PreviewDatalink(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> GetReaderData(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> PreviewProfile(RemoteMessage message, CancellationToken cancellationToken);
        Task<List<TransformWriterResult>> GetResults(RemoteMessage message, CancellationToken cancellationToken);
        Task<bool> CreateFilePaths(RemoteMessage message, CancellationToken cancellationToken);
        Task<bool> MoveFiles(RemoteMessage message, CancellationToken cancellationToken);
        Task<bool> DeleteFiles(RemoteMessage message, CancellationToken cancellationToken);
        Task<List<DexihFileProperties>> GetFileList(RemoteMessage message, CancellationToken cancellationToken);
        // Task<bool> SaveFile(RemoteMessage message, CancellationToken cancellationToken);
        Task<string> UploadFile(RemoteMessage message, CancellationToken cancellationToken);
        Task<ManagedTask> DownloadFiles(RemoteMessage message, CancellationToken cancellationToken);
        Task<ManagedTask> DownloadData(RemoteMessage message, CancellationToken cancellationToken);

        NamingStandards NamingStandards(RemoteMessage message, CancellationToken cancellationToken);
        
    }
}