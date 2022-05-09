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
            db.Open(path);
            
            //for (var i = 0; i < 3000000; i++)
            //{
            //    db.Put(Encoding.UTF8.GetBytes($"test.{i}"), BitConverter.GetBytes(i));
            //    db.Put(Encoding.UTF8.GetBytes($"max"), BitConverter.GetBytes(i));
            //}
            var x = BitConverter.ToInt32(db.Get(Encoding.UTF8.GetBytes("max")));

            Console.WriteLine($"Value = {x}");
        }
    }
}
