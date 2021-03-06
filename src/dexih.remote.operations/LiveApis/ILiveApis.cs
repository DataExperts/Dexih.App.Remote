using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.MessageHelpers;


namespace dexih.remote.operations
{
    public interface ILiveApis
    {
        /// <summary>
        /// Add a new transform to the live api's
        /// </summary>
        /// <param name="apiKey"></param>
        /// <param name="transform"></param>
        /// <param name="hubKey"></param>
        /// <param name="cacheRefreshInterval"></param>
        /// <param name="securityKey"></param>
        /// <param name="selectQuery"></param>
        /// <returns></returns>
        string Add(long hubKey, long apiKey, Transform transform, TimeSpan? cacheRefreshInterval, string securityKey, SelectQuery selectQuery);

        /// <summary>
        /// Remove a transform from the live api's
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="apiKey"></param>
        /// <returns></returns>
        void Remove(long hubKey, long apiKey);

        /// <summary>
        /// Reset the caching of an existing api
        /// </summary>
        /// <returns></returns>
        bool ResetCache(long hubKey, long apiKey);

        IEnumerable<ApiData> HubApis(long hubKey);
        
        IEnumerable<ApiData> ActiveApis();

        Task<string> Query(string securityKey, string action, string queryString, string ipAddress, CancellationToken cancellationToken = default);

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