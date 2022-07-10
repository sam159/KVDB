using Microsoft.AspNetCore.Mvc;
using System.Text;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace KVDB.WebAPI.Controllers
{
    [Route("")]
    [ApiController]
    public class MainController : ControllerBase
    {
        private Database db;
        private Encoding encoding = Encoding.UTF8;

        public MainController(Database db)
        {
            this.db = db;
        }

        [HttpGet]
        public IEnumerable<string> Get(string? filter)
        {
            return db.Keys.Select(x => encoding.GetString(x)).Where(x => string.IsNullOrEmpty(filter) || x.StartsWith(filter));
        }

        [HttpGet("{key}")]
        
        public IActionResult GetValue(string key)
        {
            var value = db.Get(encoding.GetBytes(key));
            if (value == null)
            {
                return NotFound();
            }
            return Content(encoding.GetString(value), "text/plain", encoding);
        }

        [HttpPut("{key}")]
        public void SetValue(string key, [FromBody] string value)
        {
            db.Put(encoding.GetBytes(key), encoding.GetBytes(value));
        }

        // DELETE api/<MainController>/5
        [HttpDelete("{key}")]
        public void Delete(string key)
        {
            db.Delete(encoding.GetBytes(key));
        }
    }
}
