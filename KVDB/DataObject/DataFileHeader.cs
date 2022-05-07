using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace KVDB.DataObject
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DataFileHeader
    {
        public uint FileID;
        public long Created;
    }
}
