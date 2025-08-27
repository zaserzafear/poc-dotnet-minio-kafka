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

            var host = builder.Build();
            host.Run();
        }
    }
}