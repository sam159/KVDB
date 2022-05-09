﻿using KVDB.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using KVDB.DataObject;

namespace KVDB
{
    public class Database : IDisposable
    {
        string dataDir;
        DataFile activeFile;
        readonly Dictionary<uint, DataFile> archiveFiles = new Dictionary<uint, DataFile>();

        uint nextFileID = 1;

        readonly KeyDirectory directory = new KeyDirectory();

        public long ActiveDataFileSizeLimit { get; set; } = 10 * 1024 * 1024; //10MB

        /// <summary>
        /// Returns all archive file and the active file in order of file id lowest first
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DataFile> ListDataFiles()
        {
            var archiveIds = new uint[archiveFiles.Count];
            archiveFiles.Keys.CopyTo(archiveIds, 0);
            Array.Sort(archiveIds);

            foreach (var fileId in archiveIds)
            {
                yield return archiveFiles[fileId];
            }
            // the active file will always have the heightest file id
            yield return activeFile;
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
                throw new Exception("Path is a file, expected directory");
            }
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            dataDir = path;

            var activeFilePath = Path.Combine(path, "active.db");
            if (File.Exists(activeFilePath))
            {
                var header = DataFile.ReadHeader(activeFilePath);
                activeFile = new DataFile(activeFilePath, header.FileID, true);
                nextFileID = header.FileID + 1;
            }
            else
            {
                activeFile = new DataFile(activeFilePath, nextFileID++, true);
            }
            archiveFiles.Clear();

            var archiveNameRe = new Regex(@"^archive\.(?<id>[0-9]+)\.db$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
            var archivePaths = Directory.GetFiles(path, "archive.*.db", SearchOption.TopDirectoryOnly);
            foreach (var archivePath in archivePaths)
            {
                var archiveName = Path.GetFileName(archivePath);
                var match = archiveNameRe.Match(archiveName);
                if (!match.Success)
                {
                    throw new Exception($"Archive file {archiveName} has an invalid name");
                }
                var fileId = uint.Parse(match.Groups["id"].Value);
                archiveFiles.Add(fileId, new DataFile(archivePath, fileId, false));
            }

            directory.Clear();
            foreach (var file in ListDataFiles())
            {
                file.Reset();
                RecordPointer pointer;
                while((pointer = file.NextPointer()) != null)
                {
                    directory.Add(pointer);
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
            if (activeFile.Size > ActiveDataFileSizeLimit)
            {
                activeFile.Archive();
                archiveFiles.Add(activeFile.FileID, activeFile);
                activeFile = new DataFile(Path.Combine(dataDir, "active.db"), nextFileID++, true);
            }
        }

        public byte[] Get(byte[] key)
        {
            var pointer = directory.Find(key);
            if (pointer == null || pointer.ValueSize == 0)
            {
                return null;
            }

            foreach (var file in ListDataFiles())
            {
                var data = file.GetFromPointer(pointer);
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

        void IDisposable.Dispose()
        {
            Close();
        }
    }
}
