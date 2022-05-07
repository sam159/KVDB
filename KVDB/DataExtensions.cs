using KVDB.DataObject;
using System;
using System.IO; 

namespace KVDB
{
    public static class DataExtensions
    {
        public static byte[] ToByteArray(this ref Record record)
        {
            using var ms = new MemoryStream(18 + record.KeySize + record.ValueSize);
            ms.Write(BitConverter.GetBytes(record.Checksum));
            ms.Write(BitConverter.GetBytes(record.Timestamp));
            ms.Write(BitConverter.GetBytes(record.KeySize));
            ms.Write(BitConverter.GetBytes(record.ValueSize));
            ms.Write(record.Key);
            ms.Write(record.Value);

            return ms.ToArray();
        }

        public static byte[] ToByteArray(this ref DataFileHeader header)
        {
            using var ms = new MemoryStream(12);
            ms.Write(BitConverter.GetBytes(header.FileID));
            ms.Write(BitConverter.GetBytes(header.Created));

            return ms.ToArray();
        }

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
