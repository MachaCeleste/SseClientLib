# Usage Example
```cs
private readonly SseClient? _sseClient;
...
_sseClient = new SseClient(endpoint, query, httpClient, keepAliveTimeout);
...
await _sseClient.ConnectAsync(cancellationToken);
```
