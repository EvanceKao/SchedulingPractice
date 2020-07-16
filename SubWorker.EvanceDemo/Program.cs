using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace SubWorker.EvanceDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = new HostBuilder()
                .ConfigureServices((context, services) =>
                {
                    services.AddHostedService<V3.EvanceSubWorkerBackgroundService>();
                })
                .Build();
            using (host)
            {
                host.Start();
                host.WaitForShutdown();
            }
        }
    }
}
