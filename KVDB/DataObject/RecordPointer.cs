namespace KVDB.DataObject
{
    public class RecordPointer
    {
        public byte[] Key;
        public uint FileID;
        public int ValueSize;
        public long ValuePosition;
        public long Timestamp;

        public bool ValueEmpty
        {
            get { return ValueSize == 0; }
        }
    }
}
