using System.Runtime.InteropServices;

namespace KVDB.DataObject
{
    [StructLayout(LayoutKind.Sequential)]
    public struct RecordPointer
    {
        public byte[] Key;
        public uint FileID;
        public int ValueSize;
        public long ValuePosition;
        public long Timestamp;
    }
}
