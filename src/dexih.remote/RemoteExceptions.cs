using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.remote
{
    public class RemoteException : Exception
    {
        public RemoteException(string message) : base(message)
        {

        }

        public RemoteException(string message, Exception ex) : base(message, ex)
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
