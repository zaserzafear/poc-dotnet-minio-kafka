using Confluent.Kafka;
using Consumer.Services;
using Contracts.Dtos;
using System.Text.Json;

namespace Consumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly MinioCleanupService _minioCleanupService;

    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        MinioCleanupService minioCleanupService)
    {
        _logger = logger;
        _topic = configuration.GetValue<string>("Kafka:Topic")!;

        var server = configuration.GetValue<string>("Kafka:BootstrapServers");
        var groupId = configuration.GetValue<string>("Kafka:GroupId");

        _config = new ConsumerConfig
        {
            BootstrapServers = server,
            GroupId = groupId,

            // ถ้าไม่มี offset เดิม → อ่านจากต้น (เพื่อไม่ตก message)
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // เราจะจัดการ commit เองแบบ batch
            EnableAutoCommit = false,

            // ไม่จำเป็นต้องรู้ EOF
            EnablePartitionEof = false,

            // ดึง message ต่อรอบได้มากขึ้น (default แค่ 50MB)
            FetchMaxBytes = 100 * 1024 * 1024,         // 100 MB
            MaxPartitionFetchBytes = 50 * 1024 * 1024, // 50 MB ต่อ partition

            // เพิ่ม throughput โดยลด overhead heartbeat
            SessionTimeoutMs = 45000,    // 45s (default 10s) กัน consumer processing นาน ๆ ไม่โดน kick ออก
            HeartbeatIntervalMs = 15000, // 15s heartbeat interval (≈ 1/3 ของ SessionTimeout)

            // กัน consumer โดน rebalance ถ้า process ช้านาน
            MaxPollIntervalMs = 300000,  // 5 นาที (ค่า default), สามารถเพิ่มถ้า process ต่อ batch ใช้เวลานาน

            // ปรับ parallelism I/O
            FetchWaitMaxMs = 100,        // รอสะสม message ก่อนส่งกลับ consumer (เพิ่ม batch size)
            FetchMinBytes = 1_048_576,   // 1 MB อย่างน้อยต่อ request → ลดจำนวน request

            // Stability
            EnableAutoOffsetStore = false, // เราจะ store offset manual หลังจาก process สำเร็จ
            AllowAutoCreateTopics = false, // ป้องกันสร้าง topic โดยไม่ได้ตั้งใจ
        };

        _logger.LogInformation("Worker initialized with server {Server} and group {GroupId}", server, groupId);
        _minioCleanupService = minioCleanupService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Kafka consumer worker...");

        using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
        consumer.Subscribe(_topic);
        _logger.LogInformation("Subscribed to topic: {Topic}", _topic);

        var lastResult = (ConsumeResult<Ignore, string>?)null;
        var lastCommitTime = DateTime.UtcNow;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);

                    if (result?.Message is null)
                        continue;

                    await ProcessMessageAsync(result.Message.Value, stoppingToken);

                    // เก็บ offset ไว้
                    consumer.StoreOffset(result);
                    lastResult = result;

                    // ถ้าเกิน 5 วินาทีแล้วตั้งแต่ commit ครั้งก่อน → commit
                    if ((DateTime.UtcNow - lastCommitTime).TotalSeconds >= 5)
                    {
                        consumer.Commit(lastResult);
                        lastCommitTime = DateTime.UtcNow;
                        _logger.LogInformation("Committed offsets up to {TopicPartitionOffset}", lastResult.TopicPartitionOffset);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Consumer operation canceled. Stopping worker...");
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
            // commit รอบสุดท้ายก่อนปิด (ถ้ามี message ที่ยังไม่ commit)
            if (lastResult is not null)
            {
                consumer.Commit(lastResult);
                _logger.LogInformation("Final commit at shutdown: {TopicPartitionOffset}", lastResult.TopicPartitionOffset);
            }

            _logger.LogInformation("Closing Kafka consumer...");
            consumer.Close();
        }
    }

    private Task ProcessMessageAsync(string message, CancellationToken token)
    {
        _logger.LogInformation("Processing message: {Message}", message);

        var data = JsonSerializer.Deserialize<MessageDto>(message);

        if (data is null)
        {
            _logger.LogWarning("Received null or invalid message data");
            return Task.CompletedTask;
        }

        _logger.LogInformation("Deleting file from Minio - Bucket: {Bucket}, Path: {Path}", data.Bucket, data.Path);
        _minioCleanupService.DeleteFileAsync(data.Bucket, data.Path, token).GetAwaiter().GetResult();
        _logger.LogInformation("File deleted successfully from Minio - Bucket: {Bucket}, Path: {Path}", data.Bucket, data.Path);
        return Task.CompletedTask;
    }
}
