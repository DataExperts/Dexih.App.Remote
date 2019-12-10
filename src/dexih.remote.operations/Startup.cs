using System;
using System.IO;
using System.Threading.Tasks;
using System.Web;
using dexih.functions;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace dexih.remote.operations
{
    public class Startup
    {
        
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            // Add Cors
            services.AddCors();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILiveApis liveApis, ISharedSettings sharedSettings, ILogger<HttpService> logger)
        {
//            if (env.IsDevelopment())
//            {
//                app.UseDeveloperExceptionPage();
//            }

            // only allow requests from the original web site.
            app.UseCors(builder =>
            {
                builder.AllowAnyOrigin()
                    .AllowAnyMethod()
                    .AllowAnyHeader()
                //    .AllowCredentials()
                    .WithHeaders()
                    .WithMethods();
                //   .WithOrigins(streams.OriginUrl);
                // .WithOrigins();
            });

            // var rand = EncryptString.GenerateRandomKey();

            app.Run(async (context) =>
            {
                async Task SendFailedResponse(ReturnValue returnValue)
                {
                    logger.LogError(returnValue.Exception, $"Path: {context.Request.Path}, Message: {returnValue.Message}");
                    context.Response.StatusCode = 400;
                    context.Response.ContentType = "application/json";

                    using (var writer = new StreamWriter(context.Response.Body))
                    {
                        await writer.WriteAsync(returnValue.Serialize());
                        await writer.FlushAsync().ConfigureAwait(false);
                    }
                }

                
                context.Features.Get<IHttpMaxRequestBodySizeFeature>().MaxRequestBodySize = 1_000_000_000;
                var path = context.Request.Path;
                var segments = path.Value.Split('/');

                switch (segments[1])
                {
                    case "ping":
                        context.Response.StatusCode = 200;
                        context.Response.ContentType = "application/json";
                        await context.Response.WriteAsync("{ \"Status\": \"Alive\"}");
                        break;
                    
                    case "setRaw":
                        try
                        {
                            var key = segments[2];
                            var value = segments[3];
                            sharedSettings.SetCacheItem(key + "-raw", value);
                        }
                        catch (Exception e)
                        {
                            var returnValue = new ReturnValue(false, "Set raw call failed: " + e.Message, e);
                            await SendFailedResponse(returnValue);
                        }

                        break;
                    case "api":
                        try
                        {
                            var key1 = HttpUtility.UrlDecode(segments[2]);

                            if (segments.Length > 3 && segments[3] == "ping")
                            {
                                var ping = liveApis.Ping(key1);
                                using (var writer = new StreamWriter(context.Response.Body))
                                {
                                    await writer.WriteAsync(ping.Serialize());
                                    await writer.FlushAsync().ConfigureAwait(false);
                                }

                                return;
                            }

                            var action = "";
                            if (segments.Length > 3)
                            {
                                action = segments[3];
                            }

                            var parameters = context.Request.QueryString.Value;
                            var ipAddress = context.Request.HttpContext.Connection.RemoteIpAddress;
                            var data = await liveApis.Query(key1, action, parameters, ipAddress.ToString());

                            context.Response.StatusCode = 200;
                            context.Response.ContentType = "application/json";
                            using (var writer = new StreamWriter(context.Response.Body))
                            {
                                await writer.WriteAsync(data);
                                await writer.FlushAsync().ConfigureAwait(false);
                            }
                        }
                        catch (Exception e)
                        {
                            var returnValue = new ReturnValue(false, "API call failed: " + e.Message, e);
                            await SendFailedResponse(returnValue);
                        }

                        break;
                    
                    case "download":
                        try
                        {
                            var key = segments[2];
                            using (var downloadStream = await sharedSettings.GetCacheItem<DownloadStream>(key))
                            {

                                if (downloadStream == null)
                                {
                                    throw new RemoteException(
                                        "Remote agent call failed, the response key was not found.");
                                }

                                switch (downloadStream.Type)
                                {
                                    case "file":
                                        context.Response.ContentType = "application/octet-stream";
                                        break;
                                    case "csv":
                                        context.Response.ContentType = "text/csv";
                                        break;
                                    case "json":
                                        context.Response.ContentType = "application/json";
                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException(
                                            $"The type {downloadStream.Type} was not recognized.");
                                }

                                if (downloadStream.IsError)
                                {
                                    context.Response.StatusCode = 400;
                                }
                                else
                                {
                                    context.Response.StatusCode = 200;
                                }

                                if (!string.IsNullOrEmpty(downloadStream.FileName))
                                {
                                    context.Response.Headers.Add("Content-Disposition",
                                        "attachment; filename=" + downloadStream.FileName);
                                }
                                await downloadStream.Stream.CopyToAsync(context.Response.Body, context.RequestAborted);
                            }
                        }
                        catch (Exception e)
                        {
                            var returnValue = new ReturnValue(false, "Remote agent error: " + e.Message, e);
                            await SendFailedResponse(returnValue);
                        }
                        break;

                    case "upload":
                        try
                        {
                            var files = context.Request.Form.Files;
                            if (files.Count >= 1)
                            {
                                var key2 = segments[2];
                                var uploadStream = await sharedSettings.GetCacheItem<Func<Stream, Task>>(key2);
                                await uploadStream.Invoke(files[0].OpenReadStream());
                            }
                            else
                            {
                                throw new Exception("The file upload only supports one file.");
                            }
                        }
                        catch (Exception e)
                        {
                            var returnValue = new ReturnValue(false, "Upload data failed: " + e.Message, e);
                            await SendFailedResponse(returnValue);
                        }
                        
                        break;
                }

            });
        }
        
    }
}
