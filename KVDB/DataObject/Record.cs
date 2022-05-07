using System;
using System.IO;
using System.Runtime.InteropServices;

namespace KVDB.DataObject
{
    [StructLayout(LayoutKind.Sequential)]
    public struct Record
    {
        public Record(Stream stream)
        {
            using var br = new BinaryReader(stream, System.Text.Encoding.Default, true);
            Checksum = br.ReadUInt32();
            Timestamp = br.ReadInt64();
            KeySize = br.ReadUInt16();
            ValueSize = br.ReadInt32();
            Key = br.ReadBytes(KeySize);
            Value = br.ReadBytes(ValueSize);
        }

        public Record(byte[] key, byte[] value)
        {
            Checksum = 0;
            Timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            KeySize = (ushort)key.Length;
            ValueSize = value.Length;
            Key = key;
            Value = value;
        }

        public uint Checksum;  // 4
        public long Timestamp; // 8
        public ushort KeySize; // 2
        public int ValueSize;  // 4
        public byte[] Key;
        public byte[] Value;
    }
}
