using Contracts.Dtos;
using Microsoft.AspNetCore.Mvc;
using Producer.Models;
using Producer.Services;

namespace Producer.Controllers;

[Route("api/[controller]")]
[ApiController]
public class ProducerController(IConfiguration configurations, MinioUploadService minioUpload, KafkaProducerService kafkaProducer) : ControllerBase
{
    [HttpPost("upload")]
    public async Task<IActionResult> Upload([FromForm] MessageRequest request, CancellationToken cancellationToken = default)
    {
        var bucketName = configurations.GetValue<string>("Minio:Bucket")!;

        await using var stream = request.File.OpenReadStream();

        await minioUpload.UploadFileAsync(
            bucketName,
            request.FullPath,
            stream,
            request.File.ContentType
        );

        var message = new MessageDto
        {
            Bucket = bucketName,
            Path = request.FullPath
        };

        await kafkaProducer.ProduceAsync(message, cancellationToken);

        return Accepted();
    }
}
