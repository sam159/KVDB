using Force.Crc32;
using KVDB.DataObject;
using KVDB.Converters;

namespace KVDB
{
    public static class Hash
    {
        public static uint ChecksumRecord(ref Record record)
        {
            byte[] data = RecordConverter.ToBytes(ref record);
            return Crc32CAlgorithm.Compute(data, 4, data.Length - 4);
        }

        public static bool ValidateRecord(Record record)
        {
            byte[] data = RecordConverter.ToBytes(ref record);
            return record.Checksum == Crc32CAlgorithm.Compute(data, 4, data.Length - 4);
        }
    }
}
