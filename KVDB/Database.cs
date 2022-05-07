using KVDB.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using KVDB.DataObject;

namespace KVDB
{
    public class Database : IDisposable
    {
        DataFile activeFile;

        Dictionary<uint, DataFile> archiveFiles = new Dictionary<uint, DataFile>();

        uint nextFileID = 1;

        readonly KeyDirectory directory = new KeyDirectory();

        public long ActiveDataFileSizeLimit { get; set; } = 10 * 1024 * 1024; //10MB

        public IEnumerable<DataFile> ListDataFiles()
        {
            yield return activeFile;
            foreach (var file in archiveFiles.Values)
            {
                yield return file;
            }
        }

        public IEnumerable<byte[]> Keys
        {
            get
            {
                foreach (var pointer in directory.Pointers)
                {
                    if (pointer.ValueSize > 0)
                    {
                        yield return pointer.Key;
                    }
                }
            }
        }

        public void Open(string path)
        {
            if (activeFile != null)
            {
                Close();
            }
            if (File.Exists(path))
            {
                var header = DataFile.ReadHeader(path);
                activeFile = new DataFile(path, header.FileID, true);
                nextFileID = header.FileID + 1;
            }
            else
            {
                activeFile = new DataFile(path, nextFileID++, true);
            }
            archiveFiles.Clear();

            directory.Clear();
            foreach (var file in ListDataFiles())
            {
                file.Reset();
                RecordPointer? pointer;
                while((pointer = file.NextPointer()) != null)
                {
                    directory.Add(pointer.Value);
                }
            }
        }

        public void Close()
        {
            activeFile.Dispose();
            activeFile = null;
            foreach (var file in archiveFiles.Values)
            {
                file.Dispose();
            }
            archiveFiles.Clear();
            directory.Clear();
            nextFileID = 1;
        }

        public void Delete(byte[] key)
        {
            Put(key, null);
        }

        public void Put(byte[] key, byte[] value)
        {
            if (value == null)
            {
                value = new byte[0];
            }
            directory.Add(activeFile.Append(new Record(key, value)));
        }

        public byte[] Get(byte[] key)
        {
            var pointer = directory.Find(key);
            if (pointer == null || pointer.Value.ValueSize == 0)
            {
                return null;
            }

            foreach (var file in ListDataFiles())
            {
                var data = file.GetFromPointer(pointer.Value);
                if (data != null)
                {
                    if (data.Length == 0)
                    {
                        return null;
                    }
                    return data;
                }
            }
            return null;
        }

        public void Dispose()
        {
            Close();
        }
    }
}
