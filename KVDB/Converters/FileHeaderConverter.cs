using KVDB.DataObject;
using System.IO;
using System.Text;


namespace KVDB.Converters
{
    public static class FileHeaderConverter
    {
        public static FileHeader FromStream(Stream stream)
        {
            using var br = new BinaryReader(stream, Encoding.UTF8, true);
            var header = new FileHeader();
            header.FileID = br.ReadUInt32();
            header.Created = br.ReadInt64();
            return header;
        }

        public static void ToStream(FileHeader header, Stream stream)
        {
            using var bw = new BinaryWriter(stream, Encoding.UTF8, true);
            bw.Write(header.FileID);
            bw.Write(header.Created);
        }
    }
}
