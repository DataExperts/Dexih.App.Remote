using dexih.repository;
using Dexih.Utils.Crypto;

namespace dexih.remote.operations
{
    public enum EAutoStartType
    {
        Datajob,
        Api
    }
    
    public class AutoStart
    {
        public EAutoStartType Type { get; set; }
        public long Key { get; set; }
        
        [JsonEncrypt]
        public string SecurityKey { get; set; }
        public DexihHub Hub { get; set; }
        
        public DexihHubVariable[] HubVariables { get; set; }
        
        [JsonEncrypt]
        public string EncryptionKey { get; set; }

    }
}