using System;

namespace dexih.remote.operations
{
    public class LiveDataException : Exception
    {
        public LiveDataException(string message) : base(message)
        {

        }

        public LiveDataException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}