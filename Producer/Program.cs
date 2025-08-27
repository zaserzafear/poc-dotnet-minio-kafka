
using Minio;
using Producer.Services;

namespace Producer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            builder.Logging.AddConsole(configure =>
            {
                configure.FormatterName = "datetime";
            });

            // Add services to the container.

            builder.Services.AddControllers();
            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            var minioEndpoint = builder.Configuration.GetValue<string>("Minio:Endpoint");
            var minioAccessKey = builder.Configuration.GetValue<string>("Minio:AccessKey");
            var minioSecretKey = builder.Configuration.GetValue<string>("Minio:SecretKey");
            var minioSecure = builder.Configuration.GetValue<bool>("Minio:Secure");

            builder.Services.AddMinio(configureClient => configureClient
            .WithEndpoint(minioEndpoint)
            .WithCredentials(minioAccessKey, minioSecretKey)
            .WithSSL(minioSecure)
            .Build());

            builder.Services.AddSingleton<MinioUploadService>();
            builder.Services.AddSingleton<KafkaProducerService>();

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseAuthorization();


            app.MapControllers();

            app.Run();
        }
    }
}
