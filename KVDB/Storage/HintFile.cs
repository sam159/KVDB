using System;
using System.Collections.Generic;
using System.IO;
using KVDB.Converters;
using KVDB.DataObject;

namespace KVDB.Storage
{
    public class HintFile : IDisposable
    {
        public static bool Exists(string dir, uint fileId)
        {
            return File.Exists(Path.Combine(dir, $"hint.{fileId}.db"));
        }

        public static void Delete(string dir, uint fileId)
        {
            var hintFileName = Path.Combine(dir, $"hint.{fileId}.db");
            if (File.Exists(hintFileName))
            {
                File.Delete(hintFileName);
            }
        }

        readonly FileStream file;
        readonly long fileSize;
        readonly FileHeader header;

        public HintFile(DataFile file, bool writable) : this(Path.GetDirectoryName(file.FilePath), file.FileID, writable)
        { }

        public HintFile(string dir, uint fileID, bool writable)
        {
            var path = Path.Combine(dir, $"hint.{fileID}.db");
            if (writable)
            {
                file = File.Open(path, FileMode.Create, FileAccess.Write, FileShare.None);
                FileHeaderConverter.ToStream(new FileHeader(fileID), file);
            }
            else
            {
                file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                fileSize = file.Length;
                header = FileHeaderConverter.FromStream(file);
                if (header.FileID != fileID)
                {
                    throw new InvalidDataException($"expected file id {fileID} but got {header.FileID}");
                }
            }
        }

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

            while (file.Position < fileSize)
            {
                var hint = HintRecordConverter.FromStream(file);
                yield return new RecordPointer()
                {
                    Key = hint.Key,
                    FileID = header.FileID,
                    Timestamp = hint.Timestamp,
                    ValueSize = hint.ValueSize,
                    ValuePosition = hint.ValuePosition
                };
            }
            scanning = false;
        }

        public void Append(RecordPointer pointer)
        {
            if (!file.CanWrite)
            {
                throw new InvalidOperationException("hint file is not open for writing");
            }
            file.Seek(0, SeekOrigin.End);
            HintRecordConverter.ToStream((HintRecord)pointer, file);
        }

        public void Dispose()
        {
            ((IDisposable)file).Dispose();
        }
    }
}
