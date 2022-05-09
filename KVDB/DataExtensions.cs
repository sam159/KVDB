using KVDB.DataObject;

namespace KVDB
{
    public static class DataExtensions
    {
        public static void UpdateChecksum(this ref Record record)
        {
            record.Checksum = Hash.ChecksumRecord(ref record);
        }

        public static bool ValidChecksum(this ref Record record)
        {
            return Hash.ValidateRecord(record);
        }
    }
}
