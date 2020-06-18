using System.Text.Json.Serialization;
using dexih.repository;
using Dexih.Utils.Crypto;

namespace dexih.remote.operations
{
    public enum EAutoStartType
    {
        Datajob = 1,
        Api
    }
    
    public class AutoStart
    {
        public EAutoStartType Type { get; set; }
        public long Key { get; set; }
        
        [JsonIgnore]
        public string SecurityKey { get; set; }
        
        public string SecurityKeyEncrypted { get; set; }
        
        public DexihHub Hub { get; set; }
        
        public string[] AlertEmails { get; set; }
        
        public DexihHubVariable[] HubVariables { get; set; }
        
        [JsonIgnore]
        public string EncryptionKey { get; set; }
        public string EncryptionKeyEncrypted { get; set; }
        
        public bool Encrypt(string key)
        {
            if(!string.IsNullOrEmpty(SecurityKey))
            {
                SecurityKeyEncrypted = EncryptString.Encrypt(SecurityKey, key);
            } 

            if(!string.IsNullOrEmpty(EncryptionKey))
            {
                EncryptionKeyEncrypted = EncryptString.Encrypt(EncryptionKey, key);
            }

            return true;
        }

        public bool Decrypt(string key)
        {
            if(!string.IsNullOrEmpty(SecurityKeyEncrypted))
            {
                SecurityKey = EncryptString.Decrypt(SecurityKeyEncrypted, key);
            } 

            if(!string.IsNullOrEmpty(EncryptionKeyEncrypted))
            {
                EncryptionKey = EncryptString.Decrypt(EncryptionKeyEncrypted, key);
            }

            return true;
        }
    }
}