using KVDB.DataObject;
using System.Collections.Generic;

namespace KVDB.Storage
{
    public class KeyDirectory
    {
        readonly Dictionary<byte[], LinkedList<RecordPointer>> pointers = new Dictionary<byte[], LinkedList<RecordPointer>>(new ByteArrayComparer());

        /// <summary>
        /// Iterates latest version of all keys
        /// </summary>
        public IEnumerable<RecordPointer> Pointers
        {
            get {
                foreach (var list in pointers.Values)
                {
                    if (list.Count > 0)
                    {
                        yield return list.Last.Value;
                    }
                }
            }
        }

        public void Clear()
        {
            pointers.Clear();
        }

        public RecordPointer Find(byte[] key)
        {
            LinkedList<RecordPointer> list;
            if (!pointers.TryGetValue(key, out list))
            {
                return null;
            }
            return list.Last?.Value;
        }

        public void Add(RecordPointer pointer)
        {
            LinkedList<RecordPointer> list;
            if (!pointers.TryGetValue(pointer.Key, out list))
            {
                list = new LinkedList<RecordPointer>();
                list.AddLast(pointer);
                pointers.Add(pointer.Key, list);
                return;
            }
            if (list.Count == 0 || list.Last.Value.Timestamp <= pointer.Timestamp)
            {
                list.AddLast(pointer);
                return;
            }
            var node = list.First;
            while (node != null)
            {
                if (node.Value.Timestamp < pointer.Timestamp)
                {
                    list.AddAfter(node, pointer);
                    break;
                }
                node = node.Next;
            }
        }

        public void Remove(RecordPointer pointer)
        {
            LinkedList<RecordPointer> list;
            if (!pointers.TryGetValue(pointer.Key, out list))
            {
                return;
            }
            var node = list.First;
            while(node != null)
            {
                if (node.Value.FileID == pointer.FileID && node.Value.ValuePosition == pointer.ValuePosition)
                {
                    list.Remove(node);
                    break;
                }
            }
            if (list.Count == 0)
            {
                pointers.Remove(pointer.Key);
            }
        }
    }
}
