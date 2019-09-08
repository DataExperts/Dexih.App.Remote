using System;
using System.IO;
using System.Threading.Tasks;
using System.Web;
using dexih.remote.Operations.Services;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;

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
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IStreams streams, ILiveApis liveApis, IMemoryCache memoryCache)
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

            var rand = EncryptString.GenerateRandomKey();

            app.Run(async (context) =>
            {
                context.Features.Get<IHttpMaxRequestBodySizeFeature>().MaxRequestBodySize = 1_000_000_000;
                var path = context.Request.Path;
                var segments = path.Value.Split('/');

                if (segments[1] == "ping")
                {
                    context.Response.StatusCode = 200;
                    context.Response.ContentType = "application/json";
                    await context.Response.WriteAsync("{ \"Status\": \"Alive\"}");
                }
                
                else if (segments[1] == "api")
                {
                    try
                    {
                        var key = HttpUtility.UrlDecode(segments[2]);

                        if (segments.Length > 3 && segments[3] == "ping")
                        {
                            var ping = liveApis.Ping(key);
                            using (var writer = new StreamWriter(context.Response.Body))
                            {
                                var result = Json.SerializeObject(ping, rand);
                                await writer.WriteAsync(result);
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
//                        var cts=  new CancellationTokenSource();
//                        cts.CancelAfter(5000);
                        var data = await liveApis.Query(key, action, parameters, ipAddress.ToString());

                        context.Response.StatusCode = 200;
                        context.Response.ContentType = "application/json";
                        using (var writer = new StreamWriter(context.Response.Body))
                        {
                            await writer.WriteAsync(data.ToString());
                            await writer.FlushAsync().ConfigureAwait(false);
                        }
                    }
                    catch (Exception e)
                    {
                        context.Response.StatusCode = 200;
                        context.Response.ContentType = "application/json";

                        var returnValue = new ReturnValue(false, "API call failed: " + e.Message, e);
                        using (var writer = new StreamWriter(context.Response.Body))
                        {
                            var result = Json.SerializeObject(returnValue, rand);
                            await writer.WriteAsync(result);
                            await writer.FlushAsync().ConfigureAwait(false);
                        }
                    }
                }
                
                else if (segments[1] == "message")
                {
                    var messageId = segments[2];

                    for(var i = 0; i< 10; i++)
                    {
                        if(memoryCache.TryGetValue(messageId, out Stream stream))
                        {
                            memoryCache.Remove(messageId);
                            await stream.CopyToAsync(context.Response.Body);
                            return;
                        }
                        await Task.Delay(100);
                    }

                    throw new Exception("The response message was not found.");
                }
                
                else if (segments.Length >= 4)
                {
                    var command = segments[1];
                    var key = HttpUtility.UrlDecode(segments[2]);
                    var securityKey = HttpUtility.UrlDecode(segments[3])?.Replace(" ", "+");

                    try
                    {

                        if (command == "upload")
                        {
                            var files = context.Request.Form.Files;
                            if (files.Count >= 1)
                            {
                                var memoryStream = new MemoryStream();
                                await files[0].CopyToAsync(memoryStream);
                                memoryStream.Position = 0;
                                await streams.ProcessUploadAction(key, securityKey, memoryStream);
                            }
                            else
                            {
                                throw new Exception("The file upload only supports one file.");
                            }
                        }
                        else
                        {
                            var downloadStream = streams.GetDownloadStream(key, securityKey);

                            switch (command)
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
                                    throw new ArgumentOutOfRangeException($"The command {command} was not recognized.");
                            }

                            context.Response.StatusCode = 200;
                            context.Response.Headers.Add("Content-Disposition", "attachment; filename=" + downloadStream.FileName);
                            await downloadStream.DownloadStream.CopyToAsync(context.Response.Body, context.RequestAborted);
                            downloadStream.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        context.Response.StatusCode = 200;
                        context.Response.ContentType = "application/json";

                        var returnValue = new ReturnValue(false, "Data reader failed with error: " + e.Message, e);
                        using (var writer = new StreamWriter(context.Response.Body))
                        {
                            var result = Json.SerializeObject(returnValue, rand);
                            await writer.WriteAsync(result);
                            await writer.FlushAsync().ConfigureAwait(false);
                        }
                    }
                }
            });
        }
        
    }
}
