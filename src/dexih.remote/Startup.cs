using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Text;
using System.Threading;
using System.Web;
using dexih.remote.Operations.Services;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

namespace dexih.remote
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
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IDownloadStreams downloadStreams, IUploadStreams uploadStreams, IBufferedStreams bufferedStreams)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }


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

            app.Run(async (context) =>
            {
                context.Features.Get<IHttpMaxRequestBodySizeFeature>().MaxRequestBodySize = 1_000_000_000;
                var path = context.Request.Path;
                var segments = path.Value.Split('/');

                if (segments.Length >= 4)
                {
                    var command = segments[1];
                    var key = HttpUtility.UrlDecode(segments[2]);
                    var securityKey = HttpUtility.UrlDecode(segments[3])?.Replace(" ", "+");

                    try
                    {

                        switch (command)
                        {
                            case "download":

                                var downloadStream = downloadStreams.GetDownloadStream(key, securityKey);

                                context.Response.Headers.Add("application/octet-stream", "");
                                context.Response.Headers.Add("Content-Disposition",
                                    "attachment; filename=" + downloadStream.fileName);

                                await downloadStream.stream.CopyToAsync(context.Response.Body);
                                downloadStream.stream.Close();

                                break;
                                
                            case "csv" :
                                
                                var downloadStream2 = downloadStreams.GetDownloadStream(key, securityKey);
                                // context.Response.Headers.Add("text/csv", "");
                                context.Response.StatusCode = 200;

                                await downloadStream2.stream.CopyToAsync(context.Response.Body);
                                downloadStream2.stream.Close();


                                break;
                                

                            case "reader":

                                var readerStream =
                                    await bufferedStreams.GetDownloadBuffer(key, securityKey, CancellationToken.None);

                                context.Response.ContentType = "application/json";

                                // use status code 206 more data available
                                context.Response.StatusCode = readerStream.moreData ? 206 : 200;

                                using (var writer = new StreamWriter(context.Response.Body))
                                {
                                    new JsonSerializer().Serialize(writer, readerStream.buffer);
                                    await writer.FlushAsync().ConfigureAwait(false);
                                }

                                break;
                            case "upload":
                                var memoryStream = new MemoryStream();
                                await context.Request.Body.CopyToAsync(memoryStream);
                                memoryStream.Position = 0;
                                await uploadStreams.ProcessUploadAction(key, securityKey, memoryStream);
                                var result = new ReturnValue(true);

                                context.Response.ContentType = "application/json";

                                using (var writer = new StreamWriter(context.Response.Body))
                                {
                                    new JsonSerializer().Serialize(writer, result);
                                    await writer.FlushAsync().ConfigureAwait(false);
                                }

                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        context.Response.StatusCode = 500;
                        context.Response.ContentType = "application/json";

                        var returnValue = new ReturnValue(false, "Data reader failed with error: " + e.Message, e);
                        using (var writer = new StreamWriter(context.Response.Body))
                        {
                            new JsonSerializer().Serialize(writer, returnValue);
                            await writer.FlushAsync().ConfigureAwait(false);
                        }
                    }

                }
            });
        }
    }
}
