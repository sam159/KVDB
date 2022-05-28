using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Linq;

namespace KVDB.Storage
{
    public class LockFile
    {
        readonly string filePath;

        public LockFile(string dir)
        {
            filePath = Path.Combine(dir, "lock.pid");
        }

        int CurrentPID => Process.GetCurrentProcess().Id;

        public int GetCurrentLockProcessId()
        {
            var lockContent = Encoding.UTF8.GetString(File.ReadAllBytes(filePath));
            int pid;
            if (!int.TryParse(lockContent, out pid))
            {
                return -1;
            }
            var processIds = Process.GetProcesses().Select(x => x.Id);
            if (!processIds.Contains(pid))
            {
                return -1;
            }
            return pid;
        }

        public bool CanLock()
        {
            var currentPid = GetCurrentLockProcessId();
            if (currentPid == -1 || currentPid == CurrentPID)
            {
                return true;
            }
            return false;
        }

        public bool Lock()
        {
            if (File.Exists(filePath))
            {
                if (!CanLock())
                {
                    return false;
                }
                File.Delete(filePath);
            }
            File.WriteAllBytes(filePath, Encoding.UTF8.GetBytes(CurrentPID.ToString()));
            return true;
        }

        public void Unlock()
        {
            if (File.Exists(filePath))
            {
                var currentPid = GetCurrentLockProcessId();
                if (currentPid == -1 || currentPid == CurrentPID)
                {
                    File.Delete(filePath);
                }
            }
        }
    }
}
