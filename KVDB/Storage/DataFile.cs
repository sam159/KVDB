using System;
using System.IO;
using KVDB.DataObject;
using KVDB.Converters;

namespace KVDB.Storage
{
    public class DataFile : IDisposable
    {
        public static DataFileHeader ReadHeader(string path)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            return DataFileHeaderConverter.FromStream(file);
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
                DataFileHeaderConverter.ToStream(header, file);
                file.Flush();
            }
            else
            {
                header = DataFileHeaderConverter.FromStream(file);

                if (header.FileID != fileID)
                {
                    throw new Exception($"File id ({header.FileID}) does not match expected id ({fileID})");
                }
            }
            Size = file.Length;
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

        public long Size { get; private set; }
        public void Reset()
        {
            file.Seek(12, SeekOrigin.Begin);
        }

        public RecordPointer NextPointer()
        {
            if (file.Position >= Size)
            {
                return null;
            }

            var record = RecordConverter.FromStream(file);
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
            var oldPosition = file.Position;
            RecordConverter.ToStream(ref record, file);
            Size += file.Position - oldPosition;
            file.Flush();
            return new RecordPointer()
            {
                Key = record.Key,
                FileID = FileID,
                Timestamp = record.Timestamp,
                ValueSize = record.ValueSize,
                ValuePosition = file.Position - record.ValueSize
            };
        }

        public void Archive()
        {
            if (file.CanWrite)
            {
                var path = file.Name;
                file.Close();
                ((IDisposable)file).Dispose();
                var newName = Path.Combine(Path.GetDirectoryName(path), $"archive.{FileID}.db");
                File.Move(path, newName);
                file = File.Open(newName, FileMode.Open, FileAccess.Read);
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
