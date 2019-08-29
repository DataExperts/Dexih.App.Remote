
namespace dexih.remote.operations
{
    public enum ERemoteOperations
    {
        Ping,
        GetRemoteAgentStatus,
        Encrypt,
        Decrypt,
        ReStart,
        TestCustomFunction,
        RunDatalinks,
        CancelDatalinks,
        CancelDatalinkTests,
        CancelTasks,
        RunDatalinkTests,
        RunDatalinkTestSnapshots,
        RunDatajobs,
        AddDataJobTask,
        ActivateDatajobs,
        ActivateDatajob,
        DeactivateDatajobs,
        ActivateApis,
        DeactivateApis,
        CallApi,
        CreateDatabase,
        RefreshConnection,
        DatabaseTableNames,
        ImportDatabaseTables,
        CreateDatabaseTables,
        ClearDatabaseTables,
        PreviewTable,
        StartUploadStream,
        PreviewTransform,
        ImportFunctionMappings,
        PreviewDatalink,
        DatalinkProperties,
        GetReaderData,
        PreviewProfile,
        GetAuditResults,
        CreateFilePaths,
        MoveFiles, 
        DeleteFiles,
        GetFileList,
        UploadFile,
        CreateTable,
        BulkUploadFiles,
        DownloadFiles,
        DownloadData,
        NamingStandards
    }
}