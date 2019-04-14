using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Threading;
using Microsoft.Extensions.Logging.Console.Internal;

namespace dexih.remote
{
    public class FileLoggerProcessor
    {
        private readonly BlockingCollection<LogMessageEntry> _messageQueue =
            new BlockingCollection<LogMessageEntry>(1024);

        private const int _maxQueuedMessages = 1024;
        private readonly Thread _outputThread;

        private readonly string _logFilePath;
        

        public FileLoggerProcessor(string path, string fileName = null)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                _logFilePath = Path.Combine(path, $"dexih-remote-{DateTime.Now:yyyyMMdd-hhmmss}.log");
            }
            else
            {
                _logFilePath = Path.Combine(path, fileName);
            }
            
            if (!File.Exists(_logFilePath)) 
            {
                // Create a file to write to.
                using (StreamWriter sw = File.CreateText(_logFilePath)) 
                {
                    sw.WriteLine($"Data Experts Remote Agent Log, started at: {DateTime.Now}");
                }	
            }
            else
            {
                using (StreamWriter sw = File.AppendText(_logFilePath)) 
                {
                    sw.WriteLine($"Data Experts Remote Agent Log, started at: {DateTime.Now}");
                }	
            }
            
            this._outputThread = new Thread(ProcessLogQueue)
            {
                IsBackground = true,
                Name = "Console logger queue processing thread"
            };
            this._outputThread.Start();
        }

        public virtual void EnqueueMessage(LogMessageEntry message)
        {
            if (!this._messageQueue.IsAddingCompleted)
            {
                try
                {
                    this._messageQueue.Add(message);
                    return;
                }
                catch (InvalidOperationException ex)
                {
                }
            }

            this.WriteMessage(message);
        }

        internal virtual void WriteMessage(LogMessageEntry message)
        {
            using (StreamWriter sw = File.AppendText(_logFilePath)) 
            {
                if (message.LevelString != null)
                    sw.Write(message.LevelString);
                sw.Write(message.Message);
                sw.Flush();
            }	
        }

        private void ProcessLogQueue()
        {
            try
            {
                foreach (LogMessageEntry consuming in _messageQueue.GetConsumingEnumerable())
                    this.WriteMessage(consuming);
            }
            catch
            {
                try
                {
                    this._messageQueue.CompleteAdding();
                }
                catch
                {
                }
            }
        }

        public void Dispose()
        {
            this._messageQueue.CompleteAdding();
            try
            {
                this._outputThread.Join(1500);
            }
            catch (ThreadStateException ex)
            {
            }
        }
    }
}