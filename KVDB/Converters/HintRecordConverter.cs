using KVDB.DataObject;
using System.IO;
using System.Text;

namespace KVDB.Converters
{
    public class HintRecordConverter
    {
        public static HintRecord FromStream(Stream stream)
        {
            var record = new HintRecord();
            using var br = new BinaryReader(stream, Encoding.Default, true);
            record.Timestamp = br.ReadInt64();
            record.KeySize = br.ReadUInt16();
            record.ValueSize = br.ReadInt32();
            record.ValuePosition = br.ReadInt64();
            record.Key = br.ReadBytes(record.KeySize);
            return record;
        }

        public static void ToStream(HintRecord record, Stream stream)
        {
            using var bw = new BinaryWriter(stream, Encoding.Default, true);
            bw.Write(record.Timestamp);
            bw.Write(record.KeySize);
            bw.Write(record.ValueSize);
            bw.Write(record.ValuePosition);
            bw.Write(record.Key);
        }
    }
}
