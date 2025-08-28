using Consumer.Services;
using Minio;

namespace Consumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);

            builder.Logging.AddConsole(configure =>
            {
                configure.FormatterName = "datetime";
            });

            builder.Services.AddHostedService<Worker>();

            var minioEndpoint = builder.Configuration.GetValue<string>("Minio:Endpoint");
            var minioAccessKey = builder.Configuration.GetValue<string>("Minio:AccessKey");
            var minioSecretKey = builder.Configuration.GetValue<string>("Minio:SecretKey");
            var minioSecure = builder.Configuration.GetValue<bool>("Minio:Secure");

            builder.Services.AddMinio(configureClient => configureClient
            .WithEndpoint(minioEndpoint)
            .WithCredentials(minioAccessKey, minioSecretKey)
            .WithSSL(minioSecure)
            .Build());

            builder.Services.AddSingleton<MinioCleanupService>();

            var host = builder.Build();
            host.Run();
        }
    }
}