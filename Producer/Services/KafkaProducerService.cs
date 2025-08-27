using Confluent.Kafka;
using System.Text.Json;

namespace Producer.Services;

public class KafkaProducerService
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly IProducer<Null, string> _producer;
    private readonly string _topic = string.Empty;

    public KafkaProducerService(
        ILogger<KafkaProducerService> logger,
        IConfiguration configuration)
    {
        _logger = logger;

        var server = configuration.GetValue<string>("Kafka:BootstrapServers");
        _topic = configuration.GetValue<string>("Kafka:Topic")!;

        var config = new ProducerConfig
        {
            BootstrapServers = server,
            AllowAutoCreateTopics = true,
            Acks = Acks.All, // producer wait for broker ack, not worker
            EnableIdempotence = true
        };
        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task ProduceAsync<T>(T data, CancellationToken cancellationToken)
    {
        try
        {

            var message = new Message<Null, string>
            {
                Value = JsonSerializer.Serialize(data)
            };

            var deliveryResult = await _producer.ProduceAsync(_topic, message, cancellationToken);

            _logger.LogInformation($"Delivered message to {deliveryResult.Value}, Offset: {deliveryResult.Offset}");
        }
        catch (ProduceException<Null, string> ex)
        {
            _logger.LogError($"Delivered failed: {ex.Error.Reason}");
        }

        _producer.Flush();
    }
}
