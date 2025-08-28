using Contracts.Dtos;
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

    public async Task DeleteFilesAsync(List<MessageDto> buffer, CancellationToken ct)
    {
        var bucket = buffer.First().Bucket;
        var paths = buffer.Select(b => b.Path).ToList();

        var args = new RemoveObjectsArgs()
                      .WithBucket(bucket)
                      .WithObjects(paths);
        await _client.RemoveObjectsAsync(args, ct);
    }
}
