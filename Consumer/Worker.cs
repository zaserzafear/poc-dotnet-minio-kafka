using Confluent.Kafka;

namespace Consumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ConsumerConfig _config;
    private readonly string _topic;

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _topic = configuration.GetValue<string>("Kafka:Topic")!;

        var server = configuration.GetValue<string>("Kafka:BootstrapServers");
        var groupId = configuration.GetValue<string>("Kafka:GroupId");
        _config = new ConsumerConfig
        {
            BootstrapServers = server,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
        consumer.Subscribe(_topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);

                    _logger.LogInformation(
                        "Consumed message at {TopicPartitionOffset}: {Message}",
                        result.TopicPartitionOffset,
                        result.Message.Value
                    );

                    await ProcessMessageAsync(result.Message.Value, stoppingToken);

                    consumer.Commit(result);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error while processing message");
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private Task ProcessMessageAsync(string message, CancellationToken token)
    {
        _logger.LogInformation("Processing message: {Message}", message);
        return Task.CompletedTask;
    }
}
