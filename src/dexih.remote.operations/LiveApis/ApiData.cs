using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion.Internal;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.remote.Operations.Services
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ApiStatus
    {
        Activated, Deactivated
    }
    
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
        
        public ApiStatus ApiStatus { get; set; }
        public long HubKey { get; set; }
        public long ApiKey { get; set; }
        public string SecurityKey { get; set; }
        public long SuccessCount { get; set; }

        public long ErrorCount { get; set; }


        [JsonIgnore]
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