using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using dexih.functions.Query;
using dexih.transforms;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using Microsoft.EntityFrameworkCore.Storage;
using Newtonsoft.Json.Linq;

namespace dexih.remote.Operations.Services
{
    public class LiveApis: ILiveApis
    {
        public event EventHandler<ApiData> OnUpdate;
        public event EventHandler<ApiQuery> OnQuery;
        

        /// <summary>
        /// Dictionary grouped by HubKeys, then ApiKeys pointing to the securityKey
        /// </summary>
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<long, string>> _hubs;
        
        /// <summary>
        /// Apis Dictionary contains security keys pointing to the transform.
        /// </summary>
        private readonly ConcurrentDictionary<string, ApiData> _liveApis;

        public LiveApis()
        {
            _liveApis = new ConcurrentDictionary<string, ApiData>();
            _hubs = new ConcurrentDictionary<long, ConcurrentDictionary<long, string>>();
        }


        public string Add(long hubKey, long apiKey, Transform transform, TimeSpan? cacheRefreshInterval, string securityKey = null)
        {
            if(!_hubs.TryGetValue(hubKey, out var apiKeys))
            {
                apiKeys = new ConcurrentDictionary<long, string>();
                _hubs.TryAdd(hubKey, apiKeys);
            }

            if (apiKeys.TryGetValue(apiKey, out var _))
            {
                throw new LiveDataException("The Api is already activated.");
            }

            if (securityKey == null)
            {
                securityKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey(25);
            }

            // strip non alphanumeric chars from the key to ensure url compatibility.
            char[] arr = securityKey.ToCharArray();
            arr = Array.FindAll<char>(arr, (c => (char.IsLetterOrDigit(c) 
                                                  || char.IsWhiteSpace(c) 
                                                  || c == '-')));
            securityKey = new string(arr);
            
            if (apiKeys.TryAdd(apiKey, securityKey))
            {
                var apiData = new ApiData()
                {
                    ApiStatus = ApiStatus.Activated,
                    HubKey = hubKey,
                    ApiKey = apiKey,
                    SecurityKey = securityKey,
                    Transform =  transform
                };

                if (cacheRefreshInterval != null)
                {
                    apiData.SetCacheResetTimer(cacheRefreshInterval.Value);
                }
                
                if (_liveApis.TryAdd(securityKey, apiData))
                {
                    OnUpdate?.Invoke(this, new ApiData() {ApiKey = apiKey, HubKey = hubKey, SecurityKey = securityKey});
                    return securityKey;
                }
                else
                {
                    // cleanup the added key if an issue occurs.
                    apiKeys.Remove(apiKey, out var _);
                }
            }

            throw new LiveDataException("The Api could not be activated.");

        }

        public void Remove(long hubKey, long apiKey)
        {
            if(_hubs.TryGetValue(hubKey, out var apiKeys))
            {
                if (apiKeys.TryGetValue(apiKey, out var securityKey))
                {
                    if (_liveApis.TryRemove(securityKey, out var apiData))
                    {
                        if (apiKeys.TryRemove(apiKey, out var _))
                        {
                            apiData.ApiStatus = ApiStatus.Deactivated;
                            OnUpdate.Invoke(this, apiData);
                            return;
                        }
                        else
                        {
                            throw new LiveDataException(
                                "The Api failed to deactivate due to an issue removing the apiKey.");
                        }
                    }
                    else
                    {
                        throw new LiveDataException(
                            "The Api failed to deactivate due to an issue removing the activated Api.");
                    }
                }
                else
                {
                    throw new LiveDataException("The Api failed to deactivate as it was not already activated.");
                }
            }        
            else
            {
                throw new LiveDataException("The Api failed to deactivate no Api's for the Hub were found.");
            }
        }

        public bool ResetCache(long hubKey, long apiKey)
        {
            if(_hubs.TryGetValue(hubKey, out var apiKeys))
            {
                if (apiKeys.TryGetValue(apiKey, out var securityKey))
                {
                    if (_liveApis.TryGetValue(securityKey, out var apiData))
                    {
                        apiData.ResetCache();
                        return true;
                    }
                }
            }

            return false;
        }

        public async Task<JObject> Query(string securityKey, string queryString, string ipAddress, CancellationToken cancellationToken = default)
        {
            if (_liveApis.TryGetValue(securityKey, out var apiData))
            {
                var timer = Stopwatch.StartNew();
                await apiData.WaitForTask(cancellationToken);

                JObject inputs = null;
                JObject query = null;

                try
                {
                    var parameters = HttpUtility.ParseQueryString(queryString);

                    // check for a query
                    var q = parameters["q"];
                    query = q == null ? null : JObject.Parse(q); 

                    var i = parameters["i"];
                    inputs = i == null ? null : JObject.Parse(i);

                    var cts = new CancellationTokenSource();
                    var t = parameters["t"];
                    if (t != null)
                    {
                        if (Int64.TryParse(t, out var timeout))
                        {
                            cts.CancelAfter(TimeSpan.FromSeconds(timeout));    
                        }
                        else
                        {
                            throw new Exception($"The timeout (t) was set to an invalid value {t}.  This needs to be the number seconds before timing out.");
                        }
                    }

                    int rows = -1;
                    var r = parameters["r"];
                    if(r != null) 
                    {
                        if (!Int32.TryParse(r, out rows))
                        {
                            throw new Exception($"The rows (r) was set to an invalid value {t}.  This needs to be the maximum number of rows.");
                        }
                    }

                        //TODO add EDownloadFormat to api options.
                    var selectQuery = new SelectQuery();
                    selectQuery.LoadJsonFilters(apiData.Transform.CacheTable, query);
                    selectQuery.LoadJsonInputs(inputs);
                    selectQuery.Rows = rows;

                    var combinedCancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token).Token;
                    var result = await apiData.Transform.LookupJson(selectQuery, Transform.EDuplicateStrategy.All, combinedCancel);
                    apiData.IncrementSuccess();
                    timer.Stop();

                    if (OnQuery != null)
                    {
//                        var inputs = selectQuery?.InputColumns?
//                            .Select(c => new KeyValuePair<string, object>(c.Name, c.DefaultValue)).ToArray();
                        
                        var queryApi = new ApiQuery()
                        {
                            HubKey =  apiData.HubKey,
                            ApiKey = apiData.ApiKey,
                            Success = true, IpAddress = ipAddress, 
                            Date = DateTime.Now, 
                            Inputs = inputs?.ToString(),
                            Filters = query?.ToString(),
                            TimeTaken = timer.ElapsedMilliseconds
                        };

                        OnQuery.Invoke(this, queryApi);
                    }

                    OnUpdate?.Invoke(this, apiData);

                    return result;
                }
                catch (Exception ex)
                {
                    apiData.IncrementError();
                    timer.Stop();
                    
                    if (OnQuery != null)
                    {
//                        var inputs = selectQuery?.InputColumns?
//                            .Select(c => new KeyValuePair<string, object>(c.Name, c.DefaultValue)).ToArray();

                        var queryApi = new ApiQuery()
                        {
                            Success = false, Message = ex.Message, Exception = ex, IpAddress = ipAddress,
                            Date = DateTime.Now, Inputs = inputs?.ToString(),
                            Filters = query?.ToString(), TimeTaken = timer.ElapsedMilliseconds
                        };

                        OnQuery?.Invoke(this, queryApi);
                        
                        var result = JToken.FromObject(new ReturnValue(false, ex.Message, ex));
                        return JObject.FromObject(result);
                    }
                    OnUpdate?.Invoke(this, apiData);
                }
                finally
                {
                    apiData.TaskComplete();
                }
            }

            throw new LiveDataException("The requested API is not available.");
        }

        public ReturnValue Ping(string securityKey)
        {
            if (_liveApis.TryGetValue(securityKey, out var apiData))
            {
                if (apiData.IsBusy())
                {
                    return new ReturnValue(true, "The Api is processing a request.", null);
                }
                else
                {
                    return new ReturnValue(true, "The Api is available.", null);
                }
            }
            else
            {
                return new ReturnValue(false, "The Api is not active.", null);
            }
        }

        public IEnumerable<ApiData> HubApis(long hubKey)
        {
            if (_hubs.TryGetValue(hubKey, out var apiKeys))
            {
                return apiKeys.Values.Select(c => _liveApis[c]).ToArray();
            }

            return new ApiData[0];
        }

        public IEnumerable<ApiData> ActiveApis()
        {
            return _liveApis.Values;
        }
    }
    


}