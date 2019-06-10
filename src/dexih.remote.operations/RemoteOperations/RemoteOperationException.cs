using System;

namespace dexih.remote.operations
{
    public class RemoteOperationException : Exception
    {
        public RemoteOperationException(string message) : base(message)
        {

        }

        public RemoteOperationException(string message, Exception ex) : base(message, ex)
        {

        }
        
        
    }
    
    public class RemoteSecurityException : Exception
    {
        public RemoteSecurityException(string message) : base(message)
        {

        }

        public RemoteSecurityException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}