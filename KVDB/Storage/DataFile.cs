using System;
using System.IO;
using KVDB.DataObject;
using KVDB.Converters;
using System.Collections.Generic;

namespace KVDB.Storage
{
    public class DataFile : IDisposable
    {
        public static FileHeader ReadHeader(string path)
        {
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            return FileHeaderConverter.FromStream(file);
        }

        FileStream file;

        FileHeader header;

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
                    throw new InvalidDataException("Data file header missing");
                }
                header = new FileHeader(fileID);
                FileHeaderConverter.ToStream(header, file);
                file.Flush();
            }
            else
            {
                header = FileHeaderConverter.FromStream(file);

                if (header.FileID != fileID)
                {
                    throw new InvalidDataException($"File id ({header.FileID}) does not match expected id ({fileID})");
                }
            }
            Size = file.Length;
        }

        bool disposed = false;
        public void Dispose()
        {
            if (!disposed)
            {
                ((IDisposable)file).Dispose();
                file = null;
                disposed = true;
            }
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
                return DateTimeOffset.FromUnixTimeMilliseconds(header.Created);
            }
        }

        public string FilePath
        {
            get
            {
                return file.Name;
            }
        }

        public long Size { get; private set; }

        private void Reset()
        {
            file.Seek(12, SeekOrigin.Begin);
        }

        private bool scanning = false;
        public IEnumerable<RecordPointer> Scan()
        {
            if (scanning)
            {
                throw new InvalidOperationException("file scan already in progress");
            }
            scanning = true;
            Reset();

            while(file.Position < Size)
            {
                var record = RecordConverter.FromStream(file);
                if (!record.ValidChecksum())
                {
                    throw new Exception("Invalid checksum");
                }
                yield return new RecordPointer()
                {
                    Key = record.Key,
                    FileID = FileID,
                    Timestamp = record.Timestamp,
                    ValueSize = record.ValueSize,
                    ValuePosition = file.Position - record.ValueSize
                };
            }
            scanning = false;
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

        public void Delete()
        {
            var path = file.Name;
            Dispose();
            File.Delete(path);
            HintFile.Delete(Path.GetDirectoryName(path), FileID);
        }

        public byte[] GetValueFromPointer(RecordPointer pointer)
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

        public Record GetRecordFromPointer(RecordPointer pointer)
        {
            if (pointer.FileID != FileID)
            {
                throw new Exception("Wrong file");
            }
            var record = new Record()
            {
                Key = pointer.Key,
                KeySize = (ushort)pointer.Key.Length,
                Timestamp = pointer.Timestamp,
                ValueSize = pointer.ValueSize,
                Value = GetValueFromPointer(pointer)
            };
            record.UpdateChecksum();
            return record;
        }

        public override bool Equals(object obj)
        {
            return obj is DataFile file &&
                   FileID == file.FileID;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(FileID);
        }
    }
}
