
namespace dexih.remote.operations
{
    public enum EConnectionResult
    {
        Connected = 1,
        Disconnected = 2,
        InvalidLocation = 3,
        InvalidCredentials = 4,
        UnhandledException = 5,
        Restart = 6,
        Connecting = 7
    }

    public enum EExitCode
    {
        Success = 1,
        InvalidSetting = 2,
        InvalidLogin = 3,
        Terminated = 4,
        UnknownError = 10,
        Upgrade = 20
    }
}