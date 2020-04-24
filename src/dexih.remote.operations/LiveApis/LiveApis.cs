using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using dexih.functions;
using dexih.functions.Query;
using dexih.operations;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using Microsoft.Extensions.Logging;


namespace dexih.remote.operations
{
    public class LiveApis: ILiveApis
    {
        private readonly ISharedSettings _sharedSettings;
        private readonly ILogger<LiveApis> _logger;
        private readonly IHttpClientFactory _clientFactory;

        /// <summary>
        /// Dictionary grouped by HubKeys, then ApiKeys pointing to the securityKey
        /// </summary>
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<long, string>> _hubs;

        private readonly ConcurrentDictionary<string, ApiData> _liveApis;
        
        private bool _apiUpdateBusy;
        private readonly ConcurrentDictionary<long, ApiData> _apiDataUpdates;

        private bool _apiQueryBusy;
        private readonly ConcurrentDictionary<long, ApiQuery> _apiQueryUpdates;

        
        public LiveApis(ISharedSettings sharedSettings, ILogger<LiveApis> logger, IHttpClientFactory clientFactory)
        {
            _sharedSettings = sharedSettings;
            _logger = logger;

            _liveApis = new ConcurrentDictionary<string, ApiData>();
            _hubs = new ConcurrentDictionary<long, ConcurrentDictionary<long, string>>();
            _apiDataUpdates = new ConcurrentDictionary<long, ApiData>();
            _apiQueryUpdates = new ConcurrentDictionary<long, ApiQuery>();
            _clientFactory = clientFactory;
        }

        
        public string Add(long hubKey, long apiKey, Transform transform, TimeSpan? cacheRefreshInterval, string securityKey, SelectQuery selectQuery)
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
                securityKey = EncryptString.GenerateRandomKey(25);
            }

            // strip non alphanumeric chars from the key to ensure url compatibility.
            var arr = securityKey.ToCharArray();
            arr = Array.FindAll<char>(arr, c => char.IsLetterOrDigit(c) 
                                                || char.IsWhiteSpace(c) 
                                                || c == '-');
            securityKey = new string(arr);
            
            if (apiKeys.TryAdd(apiKey, securityKey))
            {
                var apiData = new ApiData()
                {
                    ApiStatus = EApiStatus.Activated,
                    HubKey = hubKey,
                    ApiKey = apiKey,
                    SecurityKey = securityKey,
                    Transform =  transform,
                    SelectQuery = selectQuery
                };

                if (cacheRefreshInterval != null)
                {
                    apiData.SetCacheResetTimer(cacheRefreshInterval.Value);
                }
                
                if (_liveApis.TryAdd(securityKey, apiData))
                {
                    ApiUpdate(new ApiData() {ApiStatus = EApiStatus.Activated, ApiKey = apiKey, HubKey = hubKey, SecurityKey = securityKey});
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
                            apiData.ApiStatus = EApiStatus.Deactivated;
                            ApiUpdate(apiData);
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

        public async Task<string> Query(string securityKey, string action, string queryString, string ipAddress, CancellationToken cancellationToken = default)
        {
            if (_liveApis.TryGetValue(securityKey, out var apiData))
            {
                var timer = Stopwatch.StartNew();
                await apiData.WaitForTask(cancellationToken);

                JsonDocument inputColumns = null;
                JsonDocument query = null;
                JsonDocument inputParameters = null;

                try
                {
                    var parameters = HttpUtility.ParseQueryString(queryString);

                    
                    if (action?.ToLower() == "info")
                    {
                        var columns = apiData.Transform.CacheTable.Columns;
                        var inputColumns2 = apiData.Transform.GetSourceReader().CacheTable.Columns.Where(c => c.IsInput);

                        var infoQuery = new
                        {
                            Success = true,
                            QueryColumns = columns,
                            InputColumns = inputColumns2
                        };

                        return JsonExtensions.Serialize(infoQuery);
                    }

                    // check for a query
                    var q = parameters["q"];
                    query = q == null ? null : JsonDocument.Parse(q); 

                    var i = parameters["i"];
                    inputColumns = i == null ? null : JsonDocument.Parse(i);

                    var p = parameters["p"];
                    inputParameters = p == null ? null : JsonDocument.Parse(p);

                    var cts = new CancellationTokenSource();
                    var t = parameters["t"];
                    if (t != null)
                    {
                        if (long.TryParse(t, out var timeout))
                        {
                            cts.CancelAfter(TimeSpan.FromSeconds(timeout));    
                        }
                        else
                        {
                            throw new Exception($"The timeout (t) was set to an invalid value {t}.  This needs to be the number seconds before timing out.");
                        }
                    }

                    var rows = -1;
                    var r = parameters["r"];
                    if(r != null) 
                    {
                        if (!int.TryParse(r, out rows))
                        {
                            throw new Exception($"The rows (r) was set to an invalid value {t}.  This needs to be the maximum number of rows.");
                        }
                    }

                    // TODO add EDownloadFormat to api options.
                    var selectQuery = apiData.SelectQuery == null ? new SelectQuery() : apiData.SelectQuery.CloneProperties<SelectQuery>();
                    selectQuery.LoadJsonFilters(apiData.Transform.CacheTable, query);
                    selectQuery.LoadJsonInputColumns(inputColumns);
                    selectQuery.LoadJsonParameters(inputParameters);
                    selectQuery.Rows = rows;

                    var combinedCancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token).Token;
                    var result = await apiData.Transform.LookupJson(selectQuery, EDuplicateStrategy.All, combinedCancel);
                    apiData.IncrementSuccess();
                    timer.Stop();
                    
                    var queryApi = new ApiQuery()
                    {
                        HubKey =  apiData.HubKey,
                        ApiKey = apiData.ApiKey,
                        Success = true, IpAddress = ipAddress, 
                        Date = DateTime.Now, 
                        InputColumns = inputColumns?.ToString(),
                        InputParameters = inputParameters?.ToString(),
                        Filters = query?.ToString(),
                        TimeTaken = timer.ElapsedMilliseconds
                    };

                    ApiQuery(queryApi);
                    ApiUpdate(apiData);

                    return result;
                }
                catch (Exception ex)
                {
                    apiData.IncrementError();
                    timer.Stop();
                    
                    var queryApi = new ApiQuery()
                    {
                        Success = false, Message = ex.Message, Exception = ex, IpAddress = ipAddress,
                        Date = DateTime.Now, InputColumns = inputColumns?.ToString(),
                        Filters = query?.ToString(), TimeTaken = timer.ElapsedMilliseconds
                    };

                    ApiQuery(queryApi);
                    
                    return JsonExtensions.Serialize(new ReturnValue(false, ex.Message, ex));
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

        private class PostApiStatus
        {
            public string SecurityToken { get; set; }
            public IEnumerable<ApiData> ApiData { get; set; }

        }

        private class PostApiQuery
        {
            public string SecurityToken { get; set; }
            public IEnumerable<ApiQuery> ApiQueries { get; set; }

        }
        
        public (string securityKey, DexihApi api) ActivateApi(AutoStart autoStart)
        {
            var dbApi = autoStart.Hub.DexihApis.SingleOrDefault(c => c.IsValid && c.Key == autoStart.Key);
            if (dbApi == null)
            {
                throw new Exception($"Api with key {autoStart.Key} was not found");
            }
            
            _logger.LogInformation("Starting API - {api}.", dbApi.Name);
            
            var settings =new TransformSettings()
            {
                HubVariables = autoStart.HubVariables,
                RemoteSettings =  _sharedSettings.RemoteSettings,
                ClientFactory = _clientFactory
            };

            var hub = autoStart.Hub;

            string key;
            Transform transform;
                        
            if (dbApi.SourceType == ESourceType.Table)
            {
                var dbTable = hub.GetTableFromKey(dbApi.SourceTableKey.Value);
                var dbConnection = hub.DexihConnections.Single(c => c.IsValid && c.Key == dbTable.ConnectionKey);

                var connection = dbConnection.GetConnection( settings);
                var table = dbTable.GetTable(hub, connection, settings);

                transform = connection.GetTransformReader(table);
            }
            else
            {
                var dbDatalink =
                    hub.DexihDatalinks.Single(c => c.IsValid && c.Key == dbApi.SourceDatalinkKey.Value);
                var transformOperations = new TransformsManager(settings);
                var runPlan = transformOperations.CreateRunPlan(hub, dbDatalink, null, null, null, null);
                transform = runPlan.sourceTransform;
            }

            
            transform.SetCacheMethod(dbApi.CacheQueries ? ECacheMethod.LookupCache : ECacheMethod.NoCache);
            key = Add(hub.HubKey, autoStart.Key, transform, dbApi.CacheResetInterval, autoStart.SecurityKey, dbApi.SelectQuery);

            return (key, dbApi);
        }

        private async void ApiUpdate(ApiData apiData)
        {
            try
            {
                if (!_apiDataUpdates.ContainsKey(apiData.ApiKey))
                {
                    _apiDataUpdates.TryAdd(apiData.ApiKey, apiData);
                }
                
                if (!_apiUpdateBusy)
                {
                    _apiUpdateBusy = true;

                    while (_apiDataUpdates.Count > 0)
                    {
                        var apiUpdates = _apiDataUpdates.Values.ToList();
                        _apiDataUpdates.Clear();
                        
                        var postData = new PostApiStatus()
                        {
                            SecurityToken =  _sharedSettings.SecurityToken,
                            ApiData = apiUpdates
                        };

                        var start = new Stopwatch();
                        start.Start();
                        var result = await _sharedSettings.PostAsync<PostApiStatus, ReturnValue>("Remote/UpdateApi", postData, CancellationToken.None);
                        start.Stop();
                        _logger.LogTrace("Send api results completed in {0}ms.", start.ElapsedMilliseconds);
                        
                        if (result.Success == false)
                        {
                            _logger.LogError(250, result.Exception,
                                "Update api results failed.  Return message was: {0}." + result.Message);
                        }

                        // wait a little while for more tasks results to arrive.
                        await Task.Delay(500);
                    }

                    _apiUpdateBusy = false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(250, ex,
                    "Update api status failed with error.  Error was: {0}." + ex.Message);
                _apiUpdateBusy = false;
            }
        }
        

        private async void ApiQuery(ApiQuery query)
        {
            try
            {
                if (!_apiQueryUpdates.ContainsKey(query.ApiKey))
                {
                    _apiQueryUpdates.TryAdd(query.ApiKey, query);
                }
                
                if (!_apiQueryBusy)
                {
                    _apiQueryBusy = true;

                    while (_apiQueryUpdates.Count > 0)
                    {
                        var apiQueries = _apiQueryUpdates.Values.ToList();
                        _apiQueryUpdates.Clear();

                        var postQuery = new PostApiQuery()
                        {
                            SecurityToken = _sharedSettings.SecurityToken,
                            ApiQueries = apiQueries
                        };
                        
                        var start = new Stopwatch();
                        start.Start();
                        var result = await _sharedSettings.PostAsync<PostApiQuery, ReturnValue>("Remote/ApiQuery", postQuery, CancellationToken.None);
                        start.Stop();
                        _logger.LogTrace("Send api query completed in {0}ms.", start.ElapsedMilliseconds);

                        if (result.Success == false)
                        {
                            _logger.LogError(250, result.Exception,
                                "Query api results failed.  Return message was: {0}." + result.Message);
                        }

                        // wait a little while for more tasks results to arrive.
                        await Task.Delay(500);
                    }

                    _apiQueryBusy = false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(250, ex,
                    "Update api query failed with error.  Error was: {0}." + ex.Message);
                _apiQueryBusy = false;
            }
        }
    }
    


}