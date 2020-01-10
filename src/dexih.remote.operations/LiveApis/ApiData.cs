using System;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;
using dexih.transforms;



namespace dexih.remote.operations
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EApiStatus
    {
        Activated = 1, Deactivated
    }
    
    [DataContract]
    public class ApiData: IDisposable
    {
        public ApiData()
        {
        }
        
        private Timer _timer;
        
        /// <summary>
        /// SemaphoreSlim is used to ensure the Api is only run one at a time.
        /// </summary>
        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        [DataMember(Order = 1)] public EApiStatus ApiStatus { get; set; }

        [DataMember(Order = 2)]
        public long HubKey { get; set; }

        [DataMember(Order = 3)]
        public long ApiKey { get; set; }

        [DataMember(Order = 4)]
        public SelectQuery SelectQuery { get; set; }

        [DataMember(Order = 5)]
        public string SecurityKey { get; set; }

        [DataMember(Order = 6)]
        public long SuccessCount { get; set; }

        [DataMember(Order = 7)]
        public long ErrorCount { get; set; }


        [JsonIgnore, IgnoreDataMember]
        public Transform Transform { get; set; }

        public void IncrementSuccess()
        {
            SuccessCount++;
        }

        public void IncrementError()
        {
            ErrorCount++;
        }

        public async Task WaitForTask(CancellationToken cancellationToken = default)
        {
            await _semaphoreSlim.WaitAsync(cancellationToken);
        }

        public void TaskComplete()
        {
            _semaphoreSlim.Release();
        }

        public void SetCacheResetTimer(TimeSpan interval)
        {
            _timer = new Timer(ResetCache, null, interval, interval);
        }

        public void ResetCache(object sender = null)
        {
            Transform.Reset(true);
        }

        public bool IsBusy()
        {
            return _semaphoreSlim.CurrentCount == 0;
        }

        public void Dispose()
        {
            _semaphoreSlim?.Dispose();
            Transform?.Dispose();
            _timer?.Dispose();
        }
    }}