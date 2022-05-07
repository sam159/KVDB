using KVDB;
using KVDB.DataObject;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace KVDB.CLI
{
    class Program
    {
        static void Main(string[] args)
        {
            var path = "test.db";

            using var db = new Database();
            db.Open(path);
            
            for (var i = 0; i < 100000; i++)
            {
                db.Put(Encoding.UTF8.GetBytes("test"), BitConverter.GetBytes(i));
            }
            var x = BitConverter.ToInt32(db.Get(Encoding.UTF8.GetBytes("test")));

            Console.WriteLine($"Value = {x}");
        }
    }
}
