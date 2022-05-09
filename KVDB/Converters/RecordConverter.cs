using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using KVDB.DataObject;

namespace KVDB.Converters
{
    public static class RecordConverter
    {
        public static Record FromStream(Stream stream)
        {
            var record = new Record();
            using var br = new BinaryReader(stream, Encoding.Default, true);
            record.Checksum = br.ReadUInt32();
            record.Timestamp = br.ReadInt64();
            record.KeySize = br.ReadUInt16();
            record.ValueSize = br.ReadInt32();
            record.Key = br.ReadBytes(record.KeySize);
            record.Value = br.ReadBytes(record.ValueSize);
            return record;
        }

        public static void ToStream(ref Record record, Stream stream)
        {
            using var bw = new BinaryWriter(stream, Encoding.Default, true);
            bw.Write(record.Checksum);
            bw.Write(record.Timestamp);
            bw.Write(record.KeySize);
            bw.Write(record.ValueSize);
            bw.Write(record.Key);
            bw.Write(record.Value);
        }

        public static Record FromBytes(byte[] data)
        {
            using var ms = new MemoryStream(data);
            return FromStream(ms);
        }

        public static byte[] ToBytes(ref Record record)
        {
            using var ms = new MemoryStream(18 + record.KeySize + record.ValueSize);
            ToStream(ref record, ms);
            return ms.ToArray();
        }
    }
}
