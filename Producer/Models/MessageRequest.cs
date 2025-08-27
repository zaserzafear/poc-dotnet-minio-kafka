namespace Producer.Models;

public class MessageRequest
{
    public IFormFile File { get; set; } = null!;
    public string FullPath { get; set; } = string.Empty;
}
