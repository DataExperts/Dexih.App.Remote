using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations;

namespace dexih.remote.operations
{
    public interface IMessageQueue: IDisposable
    {
        void Add(ResponseMessage message);

        void AddResponse(string id, RemoteMessage message);

        Task WaitForMessage(CancellationToken cancellationToken);
        
        int Count { get; }
        
        bool TryDeque(out ResponseMessage result);

    }

    public class MessageQueue : IMessageQueue
    {
        private readonly ConcurrentQueue<ResponseMessage> _messageQueue; 
        private readonly ConcurrentDictionary<string, RemoteMessage> _responseMessages = new ConcurrentDictionary<string, RemoteMessage>(); //list of responses returned from clients.  This is updated by the hub.
        private readonly SemaphoreSlim _waitForMessage = new SemaphoreSlim(1, 1);

        public MessageQueue()
        {
            _messageQueue = new ConcurrentQueue<ResponseMessage>();
        }
        
        public void Add(ResponseMessage message)
        {
            _messageQueue.Enqueue(message);
            _waitForMessage.Release();
        }

        public void AddResponse(string id, RemoteMessage message)
        {
            _responseMessages.TryAdd(id, message);
        }

        public int Count => _messageQueue.Count;

        public bool TryDeque(out ResponseMessage result)
        {
            return _messageQueue.TryDequeue(out result);
        }
        public Task WaitForMessage(CancellationToken cancellationToken)
        {
            return _waitForMessage.WaitAsync(cancellationToken);
        }
        
        public void Dispose()
        {
            _waitForMessage.Dispose();
        }
    }
}