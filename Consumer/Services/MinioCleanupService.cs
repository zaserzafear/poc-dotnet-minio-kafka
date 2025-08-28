using Minio;
using Minio.DataModel.Args;

namespace Consumer.Services;

public class MinioCleanupService
{
    private readonly IMinioClient _client;

    public MinioCleanupService(IMinioClient client)
    {
        _client = client;
    }

    public async Task DeleteFileAsync(string bucket, string path, CancellationToken cancellationToken)
    {
        var removeObjectArgs = new RemoveObjectArgs()
                                .WithBucket(bucket)
                                .WithObject(path);

        await _client.RemoveObjectAsync(removeObjectArgs, cancellationToken);
    }
}
