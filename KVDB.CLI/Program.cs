using System;
using System.Text;

namespace KVDB.CLI
{
    class Program
    {
        static void Main(string[] args)
        {
            var path = "test";

            using var db = new Database();
            db.ActiveDataFileSizeLimit = 10000;
            db.Open(path);

            for (var i = 0; i < 10000; i++)
            {
                db.Put(Encoding.UTF8.GetBytes($"test.{i}"), BitConverter.GetBytes((byte)i % 255));
                db.Put(Encoding.UTF8.GetBytes($"max"), BitConverter.GetBytes(i));
            }
            db.MergeArchives();

            for (var i = 0; i < 10000; i++)
            {
                var val = BitConverter.ToInt32(db.Get(Encoding.UTF8.GetBytes($"test.{i}")));
                if ((byte)i % 255 != val)
                {
                    Console.WriteLine($"{i} = {val}");
                }
            }

            var max = BitConverter.ToInt32(db.Get(Encoding.UTF8.GetBytes("max")));

            Console.WriteLine($"Value = {max}");
        }
    }
}
