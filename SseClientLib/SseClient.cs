using System.Net.Http.Headers;

namespace SseClientLib;

public class SseClient : IDisposable
{
    private readonly HttpClient _client;
    private readonly Uri _endpoint;
    private readonly CancellationTokenSource _cts = new();
    private bool _disposed;

    private readonly TimeSpan _keepAliveTimeout;

    public event Action? OnConnected;
    public event Action? OnDisconnected;

    public event Action? OnPing;
    public event Action<string>? OnRawMessageReceived;

    public event Action<string>? OnLog;
    public event Action<string, Exception>? OnError;

    public SseClient(string endpointBase, string query, TimeSpan? keepAliveTimeout = null)
    {
        if (string.IsNullOrWhiteSpace(endpointBase))
            throw new ArgumentException("Endpoint cannot be null or empty.", nameof(endpointBase));

        if (!Uri.TryCreate(endpointBase, UriKind.Absolute, out _))
            throw new ArgumentException("[SSE] Endpoint must be an absolute URL.", nameof(endpointBase));

        var builder = new UriBuilder(endpointBase);
        var existingQuery = builder.Query.TrimStart('?');

        builder.Query = string.IsNullOrEmpty(existingQuery)
            ? query : $"{existingQuery}&{query}";

        _endpoint = builder.Uri;
        _keepAliveTimeout = keepAliveTimeout ?? TimeSpan.FromSeconds(30);
        _client = new HttpClient();
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);

        using var request = new HttpRequestMessage(HttpMethod.Get, _endpoint);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));

        OnLog?.Invoke($"[SSE] Connecting...");

        HttpResponseMessage response;
        try
        {
            response = await _client.SendAsync(request,
                HttpCompletionOption.ResponseHeadersRead,
                linkedCts.Token);
        }
        catch (OperationCanceledException)
        {
            OnLog?.Invoke($"[SSE] Connection canceled.");
            throw;
        }
        catch (Exception ex)
        {
            OnError?.Invoke($"[SSE] Connection Error: {ex.Message}", ex);
            throw;
        }

        if (!response.IsSuccessStatusCode)
        {
            var ex = new HttpRequestException($"[SSE] HTTP {(int)response.StatusCode} {response.StatusCode}");
            OnError?.Invoke(ex.Message, ex);
            throw ex;
        }

        var contentType = response.Content.Headers.ContentType?.MediaType;
        if (contentType != "text/event-stream")
            OnLog?.Invoke($"[SSE] Warning: Expected 'text/event-stream', gor '{contentType}'");

        OnLog?.Invoke($"[SSE] Connected!");
        OnConnected?.Invoke();

        try
        {
            await ReadStreamAsync(response, linkedCts.Token);
        }
        catch (OperationCanceledException)
        {
            OnLog?.Invoke("[SSE] Stream reading canceled.");
            throw;
        }
        finally
        {
            response.Dispose();
            OnLog?.Invoke("[SSE] Disconnected!");
            OnDisconnected?.Invoke();
        }
    }

    private async Task ReadStreamAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var reader = new StreamReader(stream);

        List<string> dataLines = [];
        while (!cancellationToken.IsCancellationRequested)
        {
            var readTask = reader.ReadLineAsync(cancellationToken).AsTask();
            var timeoutTask = Task.Delay(_keepAliveTimeout, cancellationToken);

            var completedTask = await Task.WhenAny(readTask, timeoutTask);

            if (completedTask == timeoutTask)
            {
                var ex = new TimeoutException($"[SSE] Keep-alive timeout: No message received for {_keepAliveTimeout.TotalSeconds}s");
                OnError?.Invoke(ex.Message, ex);
                throw ex;
            }

            string? line;
            try
            {
                line = await readTask;
            }
            catch (OperationCanceledException)
            {
                throw;
            }

            if (line is null)
                break;

            if (line.Length == 0)
            {
                DispatchMessage(dataLines);
                continue;
            }

            if (line.StartsWith(':'))
            {
                if (line.Contains("ping", StringComparison.OrdinalIgnoreCase))
                    OnPing?.Invoke();
                continue;
            }

            if (line.StartsWith("data:", StringComparison.Ordinal))
            {
                var data = line[5..];
                if (data.StartsWith(' '))
                    data = data[1..];

                dataLines.Add(data);
            }
        }

        DispatchMessage(dataLines);
    }

    private void DispatchMessage(List<string> dataLines)
    {
        if (dataLines.Count == 0)
            return;

        var message = string.Join("\n", dataLines);
        dataLines.Clear();

        OnRawMessageReceived?.Invoke(message);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cts.Cancel();
        _cts.Dispose();
        _client.Dispose();
    }
}
