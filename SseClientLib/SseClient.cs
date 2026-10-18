
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;

namespace SseClientLib
{

    public class SseClient : IDisposable
    {
        private readonly HttpClient _client;
        private readonly Uri _endpoint;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private bool _disposed;

        public event Action OnConnected;
        public event Action OnDisconnected;

        public event Action<string> OnRawMessageReceived;

        public event Action<string> OnLog;
        public event Action<string, Exception> OnError;

        public SseClient(string endpointBase, string query)
        {
            if (!Uri.TryCreate(endpointBase, UriKind.Absolute, out var uri))
                throw new ArgumentException("[SSE] Endpoint must be an absolute URL.", nameof(endpointBase));

            var builder = new UriBuilder(endpointBase);
            var existingQuery = builder.Query.TrimStart('?');
            var endpointQuery = query;

            builder.Query = string.IsNullOrEmpty(existingQuery)
                ? endpointQuery
                : $"{existingQuery}&{endpointQuery}";

            _endpoint = builder.Uri;
            _client = new HttpClient();
        }

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);

            var request = new HttpRequestMessage(HttpMethod.Get, _endpoint);
            request.Headers.Accept.Add(
                new MediaTypeWithQualityHeaderValue("text/event-stream"));

            OnLog?.Invoke($"[SSE] Connecting...");

            HttpResponseMessage response;
            try
            {
                response = await _client.SendAsync(request,
                    HttpCompletionOption.ResponseHeadersRead,
                    linkedCts.Token);
            }
            catch (Exception ex)
            {
                OnError?.Invoke($"[SSE] Connection Error: {ex.Message}", ex);
                throw;
            }

            if (!response.IsSuccessStatusCode)
            {
                var ex = new HttpRequestException($"{(int)response.StatusCode} {response.StatusCode}");
                OnError?.Invoke($"[SSE] HTTP Error: {ex.Message}", ex);
                throw ex;
            }

            response.EnsureSuccessStatusCode();

            OnLog?.Invoke($"[SSE] Connected!");
            OnConnected?.Invoke();

            var stream = await response.Content.ReadAsStreamAsync();
            var reader = new StreamReader(stream);

            List<string> dataLines = new List<string>();
            while (!reader.EndOfStream && !linkedCts.Token.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync();

                if (line is null)
                    break;

                if (line.Length == 0)
                {
                    DispatchMessage(dataLines);
                    continue;
                }

                if (line.StartsWith(":"))
                    continue;

                if (line.StartsWith("data:", StringComparison.Ordinal))
                {
                    var data = line.Substring(5);
                    if (data.StartsWith(" "))
                        data = data.Substring(1);

                    dataLines.Add(data);
                }
            }

            DispatchMessage(dataLines);
            OnLog?.Invoke("[SSE] Disconnected!");
            OnDisconnected?.Invoke();
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
}