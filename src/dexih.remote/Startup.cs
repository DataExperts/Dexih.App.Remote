using dexih.remote.Operations.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace dexih.remote
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IDownloadStreams downloadStreams)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.Run(async (context) =>
            {
                var path = context.Request.Path;
                var segments = path.Value.Split('/');

                if (segments.Length >= 4)
                {
                    var command = segments[1];
                    switch (command)
                    {
                        case "download":
                            var key = segments[2];
                            var securityKey = segments[3];

                            var stream = downloadStreams.GetDownloadStream(key, securityKey);

                            context.Response.Headers.Add("application/octet-stream", "");
                            context.Response.Headers.Add("Content-Disposition", "attachment; filename=" + stream.fileName);
                            await stream.stream.CopyToAsync(context.Response.Body);
                            
                            break;
                    }
                }
                await context.Response.WriteAsync(path.Value);
            });
        }
    }
}
