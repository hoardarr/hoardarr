using Mono.Unix.Native;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hoardarr
{
    public class Disk
    {
        public string PhysicalPath {  get; }
        public Stat MountStats { set; get; }
        public Statvfs DiskStats { set; get; }

        public DateTimeOffset? LastUse { get; private set; }

        public void Touch(bool reset = false)
        {
            lock (this)
            {
                LastUse = reset?null:DateTimeOffset.UtcNow;
            }
        }

        public Disk(string root, Stat mountStats, Statvfs diskStats)
        {
            PhysicalPath = root;
            MountStats = mountStats;
            DiskStats = diskStats;
        }

        public void UpdateDiskStats(Logger logger)
        {
            if (Syscall.statvfs(PhysicalPath, out Statvfs newDiskStats) == -1)
            {
                logger.Error($"Failed to update disk stats for {PhysicalPath}");
            }
            else
            {
                lock (this)
                    DiskStats = newDiskStats;
            }
        }
    }
}
