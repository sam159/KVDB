using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using KVDB.DataObject;
using System.Runtime.InteropServices;

namespace KVDB.Storage
{
    public class DataFile : IDisposable
    {
        public static DataFileHeader ReadHeader(string path)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            var header = new DataFileHeader();
            var headerData = new byte[12];
            file.Read(headerData);
            header.FileID = BitConverter.ToUInt32(headerData, 0);
            header.Created = BitConverter.ToInt64(headerData, 4);
            return header;
        }

        FileStream file;

        DataFileHeader header;

        public DataFile(string path, uint fileID, bool writeable)
        {
            if (writeable)
            {
                file = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            }
            else
            {
                file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            }
            if (file.Length == 0)
            {
                if (!writeable)
                {
                    throw new Exception("Data file header missing");
                }
                header = new DataFileHeader()
                {
                    FileID = fileID,
                    Created = DateTimeOffset.Now.ToUnixTimeSeconds()
                };
                file.Write(header.ToByteArray());
            }
            else
            {
                header = new DataFileHeader();
                var headerData = new byte[12];
                file.Read(headerData);
                header.FileID = BitConverter.ToUInt32(headerData, 0);
                header.Created = BitConverter.ToInt64(headerData, 4);

                if (header.FileID != fileID)
                {
                    throw new Exception($"File id ({header.FileID}) does not match expected id ({fileID})");
                }
            }
        }

        public void Dispose()
        {
            ((IDisposable)file).Dispose();
            file = null;
        }

        public uint FileID
        {
            get
            {
                return header.FileID;
            }
        }

        public DateTimeOffset Created
        {
            get
            {
                return DateTimeOffset.FromUnixTimeSeconds(header.Created);
            }
        }

        public long Size
        {
            get
            {
                return file.Length;
            }
        }

        public void Reset()
        {
            file.Seek(12, SeekOrigin.Begin);
        }

        public RecordPointer? NextPointer()
        {
            if (file.Position >= file.Length)
            {
                return null;
            }

            var record = new Record(file);
            if (!record.ValidChecksum())
            {
                throw new Exception("Invalid checksum");
            }
            return new RecordPointer()
            {
                Key = record.Key,
                FileID = FileID,
                Timestamp = record.Timestamp,
                ValueSize = record.ValueSize,
                ValuePosition = file.Position - record.ValueSize
            };
        }

        public RecordPointer Append(Record record)
        {
            if (!file.CanWrite)
            {
                throw new Exception("data file is not writable");
            }
            record.UpdateChecksum();
            file.Seek(0, SeekOrigin.End);
            file.Write(record.ToByteArray());
            return new RecordPointer()
            {
                Key = record.Key,
                FileID = FileID,
                Timestamp = record.Timestamp,
                ValueSize = record.ValueSize,
                ValuePosition = file.Position - record.ValueSize
            };
        }

        public void Lock()
        {
            if (file.CanWrite)
            {
                var path = file.Name;
                file.Close();
                ((IDisposable)file).Dispose();
                file = File.Open(path, FileMode.Open, FileAccess.Read);
            }
        }

        public byte[] GetFromPointer(RecordPointer pointer)
        {
            if (pointer.FileID != FileID)
            {
                return null;
            }
            file.Seek(pointer.ValuePosition, SeekOrigin.Begin);
            byte[] value = new byte[pointer.ValueSize];
            file.Read(value);
            return value;
        }
    }
}
