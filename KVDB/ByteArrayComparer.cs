using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace KVDB
{
    public class ByteArrayComparer : IComparer<byte[]>
    {
        public int Compare([AllowNull] byte[] x, [AllowNull] byte[] y)
        {
            // Sort nulls first
            if (x == null && y == null)
            {
                return 0;
            }
            if (x == null && y != null)
            {
                return -1;
            }
            if (x != null && y == null)
            {
                return 1;
            }
            for (int i = 0; i < x.Length; i++)
            {
                if (i >= y.Length)
                {
                    return 1;
                }
                if (x[i] < y[i])
                {
                    return -1;
                }
                if (x[i] > y[i])
                {
                    return 1;
                }
            }
            return 0;
        }
    }
}
