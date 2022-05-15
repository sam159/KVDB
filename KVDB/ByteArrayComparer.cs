using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace KVDB
{
    public class ByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
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

        public bool Equals([AllowNull] byte[] x, [AllowNull] byte[] y)
        {
            if (x == null && y == null)
            {
                return true;
            }
            return Enumerable.SequenceEqual(x, y);
        }

        public int GetHashCode([DisallowNull] byte[] obj)
        {
            var hashCode = 0;
            for (int i = 0; i < obj.Length; i++)
            {
                hashCode = HashCode.Combine(hashCode, obj[i]);
            }
            return hashCode;
        }
    }
}
