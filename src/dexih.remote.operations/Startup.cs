using System;
using System.IO;
using System.Web;
using dexih.remote.Operations.Services;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
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
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IStreams streams)
        {
//            if (env.IsDevelopment())
//            {
//                app.UseDeveloperExceptionPage();
//            }


            // only allow requests from the original web site.
            app.UseCors(builder =>
            {
                builder.AllowAnyOrigin() //    .WithOrigins(uploadStreams.OriginUrl)
                    .AllowAnyMethod()
                    .AllowAnyHeader()
                    .AllowCredentials()
                    .WithHeaders()
                    .WithMethods()
                    .WithOrigins();
            });

            var rand = EncryptString.GenerateRandomKey();

            app.Run(async (context) =>
            {
                context.Features.Get<IHttpMaxRequestBodySizeFeature>().MaxRequestBodySize = 1_000_000_000;
                var path = context.Request.Path;
                var segments = path.Value.Split('/');

                if (segments[1] == "ping")
                {
                    await context.Response.WriteAsync("{ \"status\": \"alive\"}");
                }

                else if (segments.Length >= 4)
                {
                    var command = segments[1];
                    var key = HttpUtility.UrlDecode(segments[2]);
                    var securityKey = HttpUtility.UrlDecode(segments[3])?.Replace(" ", "+");

                    try
                    {
                        var downloadStream = streams.GetDownloadStream(key, securityKey);

                        switch (command)
                        {
                            case "file":
                                context.Response.ContentType = "application/octet-stream";
                                break;
                            case "csv" :
                                context.Response.ContentType = "text/csv";
                                break;
                            case "json" :
                                context.Response.ContentType = "application/json";
                                break;
                            default:
                                throw new ArgumentOutOfRangeException($"The command {command} was not recognized.");
                        }
                        
                        context.Response.StatusCode = 200;
                        context.Response.Headers.Add("Content-Disposition", "attachment; filename=" + downloadStream.fileName);
                        await downloadStream.stream.CopyToAsync(context.Response.Body);
                        downloadStream.stream.Close();


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
