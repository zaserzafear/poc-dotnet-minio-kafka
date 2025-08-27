using Minio;
using Minio.DataModel.Args;

namespace Producer.Services;

public class MinioUploadService
{
    private readonly IMinioClient _client;

    public MinioUploadService(IMinioClient client)
    {
        _client = client;
    }

    public async Task UploadFileAsync(string bucketName, string objectPath, Stream data, string contentType)
    {
        var putObjectArgs = new PutObjectArgs()
                    .WithBucket(bucketName)
                    .WithObject(objectPath)
                    .WithStreamData(data)
                    .WithContentType(contentType);
        await _client.PutObjectAsync(putObjectArgs).ConfigureAwait(false);
    }
}
