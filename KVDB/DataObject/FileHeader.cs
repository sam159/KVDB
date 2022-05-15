using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace KVDB.DataObject
{
    [StructLayout(LayoutKind.Sequential)]
    public struct FileHeader
    {
        public FileHeader(uint fileId)
        {
            FileID = fileId;
            Created = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        }

        public uint FileID;
        public long Created;
    }
}
