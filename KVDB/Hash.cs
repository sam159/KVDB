using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Force.Crc32;
using KVDB.DataObject;

namespace KVDB
{
    public static class Hash
    {
        public static uint ChecksumRecord(ref Record record)
        {
            byte[] data = record.ToByteArray();
            return Crc32CAlgorithm.Compute(data, 4, data.Length - 4);
        }

        public static bool ValidateRecord(Record record)
        {
            byte[] data = record.ToByteArray();
            return record.Checksum == Crc32CAlgorithm.Compute(data, 4, data.Length - 4);
        }
    }
}
