using VDS.Common.Trees;
using KVDB.DataObject;
using System.Collections.Generic;

namespace KVDB.Storage
{
    public class KeyDirectory
    {
        readonly AVLTree<byte[], RecordPointer> pointers;

        public KeyDirectory()
        {
            pointers = new AVLTree<byte[], RecordPointer>(new ByteArrayComparer());
        }

        public IEnumerable<RecordPointer> Pointers
        {
            get
            {
                return pointers.Values;
            }
        }

        public void Clear()
        {
            pointers.Clear();
        }
        public RecordPointer? Find(byte[] key)
        {
            var result = pointers.Find(key);
            if (result != null)
            {
                return result.Value;
            }
            return null;
        }

        public void Add(RecordPointer pointer)
        {
            var result = pointers.Find(pointer.Key);
            if (result != null)
            {
                if (pointer.Timestamp >= result.Value.Timestamp)
                {
                    result.Value = pointer;
                }
            }
            else
            {
                pointers.Add(pointer.Key, pointer);
            }
        }
    }
}
