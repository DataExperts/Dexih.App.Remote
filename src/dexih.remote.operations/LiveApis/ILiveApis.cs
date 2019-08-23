using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;
using dexih.remote.operations;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.MessageHelpers;
using Newtonsoft.Json.Linq;

namespace dexih.remote.Operations.Services
{
    public interface ILiveApis
    {
        /// <summary>
        /// Add a new transform to the live api's
        /// </summary>
        /// <param name="transform"></param>
        /// <returns></returns>
        string Add(long hubKey, long apiKey, Transform transform, TimeSpan? cacheRefreshInterval, string securityKey, SelectQuery selectQuery);

        /// <summary>
        /// Remove a transform from the live api's
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        void Remove(long hubKey, long apiKey);

        /// <summary>
        /// Reset the caching of an existing api
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        bool ResetCache(long hubKey, long apiKey);

        IEnumerable<ApiData> HubApis(long hubKey);
        
        IEnumerable<ApiData> ActiveApis();

        Task<JObject> Query(string securityKey, string action, string queryString, string ipAddress, CancellationToken cancellationToken = default);

        ReturnValue Ping(string securityKey);

        (string securityKey, DexihApi api) ActivateApi(AutoStart autoStart);
    }

    public class ApiQuery: ReturnValue
    {
        public long HubKey { get; set; }
        public long ApiKey { get; set; }
        public string IpAddress { get; set; }
        public DateTime Date { get; set; }
        public long TimeTaken { get; set; }
        
        public string Filters { get; set; }
        public string InputColumns { get; set; }
        public string InputParameters { get; set; }
    }
}