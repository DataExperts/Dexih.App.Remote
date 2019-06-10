
namespace dexih.remote.operations
{
    public enum EConnectionResult
    {
        Connected = 0,
        Disconnected = 1,
        InvalidLocation = 2,
        InvalidCredentials = 3,
        UnhandledException = 4,
        Restart = 5,
        Connecting = 6
    }

    public enum EExitCode
    {
        Success = 0,
        InvalidSetting = 1,
        InvalidLogin = 2,
        Terminated = 3,
        UnknownError = 10,
        Upgrade = 20
    }
}