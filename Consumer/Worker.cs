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

        // buffer เก็บ path ไว้สำหรับ batch delete
        var buffer = new List<MessageDto>();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(1000));

                    if (result?.Message != null)
                    {
                        var data = JsonSerializer.Deserialize<MessageDto>(result.Message.Value);
                        if (data != null)
                        {
                            buffer.Add(data);
                            consumer.StoreOffset(result);
                            lastResult = result;
                        }
                    }

                    // เงื่อนไข batch delete: เมื่อ buffer > 1000 ไฟล์ หรือ ผ่านไป 5 วินาที
                    if (buffer.Count > 0 && (buffer.Count >= 1000 || (DateTime.UtcNow - lastCommitTime).TotalSeconds >= 5))
                    {
                        await FlushBatchAsync(buffer, consumer, lastResult!, stoppingToken);
                        lastCommitTime = DateTime.UtcNow;
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
            // flush รอบสุดท้าย
            if (buffer.Count > 0 && lastResult is not null)
            {
                await FlushBatchAsync(buffer, consumer, lastResult, stoppingToken);
            }

            _logger.LogInformation("Closing Kafka consumer...");
            consumer.Close();
        }
    }

    private async Task FlushBatchAsync(
        List<MessageDto> buffer,
        IConsumer<Ignore, string> consumer,
        ConsumeResult<Ignore, string> lastResult,
        CancellationToken token)
    {
        try
        {
            _logger.LogInformation("Batch deleting {Count} files", buffer.Count);

            await _minioCleanupService.DeleteFilesAsync(buffer, token);

            consumer.Commit(lastResult);
            _logger.LogInformation("Committed offsets after batch delete up to {Offset}", lastResult.TopicPartitionOffset);

            buffer.Clear();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during batch delete from Minio");
        }
    }
}
