namespace KVDB.DataObject
{
    public class HintRecord
    {
        public long Timestamp;
        public ushort KeySize;
        public int ValueSize;
        public long ValuePosition;
        public byte[] Key;

        public static explicit operator HintRecord(RecordPointer pointer)
        {
            return new HintRecord()
            {
                Timestamp = pointer.Timestamp,
                KeySize = (ushort)pointer.Key.Length,
                ValueSize = pointer.ValueSize,
                ValuePosition = pointer.ValuePosition,
                Key = pointer.Key
            };
        }
    }
}
