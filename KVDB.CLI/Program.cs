using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CommandLine;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO;

namespace KVDB.CLI
{
    class Program
    {
        /* Commands
         * Load from json
         * Dump to json
         * Keys
         * Set
         * Get
         * Merge
         * 
         * Options
         * Data file
         * Encoding
         * 
         */

        class DefaultOptions
        {
            [Option('p', "path", Default = "data", HelpText = "path to data dir")]
            public string DataDir { get; set; }

            [Option('e', "encoding", Default = "utf-8", HelpText = "the encoding to use for keys and values")]
            public string Encoding { get; set; }

            public Encoding GetEncoding()
            {
                return System.Text.Encoding.GetEncoding(Encoding);
            }

            public Database GetDatabase()
            {
                var db = new Database();
                db.Open(DataDir);
                return db;
            }
        }

        [Verb("dump", HelpText = "dumps database content to a json file")]
        class DumpOptions : DefaultOptions
        {
            [Value(0, Default = "dump.json", HelpText = "json file to dump db to")]
            public string OutputFile { get; set; }
        }

        [Verb("load", HelpText = "loads database with content from a json file")]
        class LoadOptions : DefaultOptions
        {
            [Value(0, Default = "dump.json", HelpText = "json file to load from")]
            public string InputFile { get; set; }
        }

        [Verb("keys", HelpText = "lists keys within the database")]
        class KeysOptions : DefaultOptions { }

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<DumpOptions, LoadOptions, KeysOptions>(args)
                .WithParsed<DumpOptions>(DumpDB)
                .WithParsed<LoadOptions>(LoadDB)
                .WithParsed<KeysOptions>(Keys)
                .WithNotParsed(errors =>
                {
                    errors.Output();
                });
        }

        static void DumpDB(DumpOptions options)
        {
            using var db = options.GetDatabase();
            var enc = options.GetEncoding();

            var data = new Dictionary<string, string>();
            foreach (var keyBytes in db.Keys)
            {
                var key = enc.GetString(keyBytes);
                var val = enc.GetString(db.Get(keyBytes));
                data.Add(key, val);
            }
            if (options.OutputFile == "-")
            {
                Console.WriteLine(JsonConvert.SerializeObject(data, Formatting.Indented));
            }
            else
            {
                File.WriteAllText(options.OutputFile, JsonConvert.SerializeObject(data, Formatting.Indented), Encoding.UTF8);
                Console.WriteLine($"Wrote {data.Count} entries to {options.OutputFile}");
            }
        }

        static void LoadDB(LoadOptions options)
        {
            using var db = options.GetDatabase();
            var enc = options.GetEncoding();

            string content;
            if (options.InputFile == "-" && Console.IsInputRedirected)
            {
                var data = new List<byte>();
                int x;
                while((x = Console.Read()) != 0)
                {
                    data.Add((byte)x);
                }
                content = Encoding.UTF8.GetString(data.ToArray());
            }
            else
            {
                content = File.ReadAllText(options.InputFile, Encoding.UTF8);
            }

            var keys = JToken
                .Parse(content)
                .Cast<KeyValuePair<string, JToken>>();

            var count = 0;
            foreach (var item in keys)
            {
                string value;
                switch (item.Value.Type)
                {
                    case JTokenType.String:
                        value = (string)item.Value;
                        break;
                    case JTokenType.Boolean:
                        value = ((bool)item.Value).ToString().ToLower();
                        break;
                    case JTokenType.Date:
                        value = ((DateTime)item.Value).ToUniversalTime().ToString("O");
                        break;
                    case JTokenType.Integer:
                        value = ((int)item.Value).ToString();
                        break;
                    case JTokenType.Float:
                        value = ((double)item.Value).ToString();
                        break;
                    case JTokenType.Null:
                        value = null;
                        break;
                    default:
                        value = item.Value.ToString();
                        break;
                }
                db.Put(
                    enc.GetBytes(item.Key),
                    value != null ? enc.GetBytes(value) : null
                );
                count++;
            }

            Console.WriteLine($"Loaded {count} from {options.InputFile}");
        }

        static void Keys(KeysOptions options)
        {
            using var db = options.GetDatabase();
            var enc = options.GetEncoding();

            foreach (var key in db.Keys)
            {
                Console.WriteLine(enc.GetString(key));
            }
        }
    }
}
