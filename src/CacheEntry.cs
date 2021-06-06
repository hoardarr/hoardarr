using Mono.Unix.Native;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hoardarr
{
    public class CacheEntry
    {
        public List<Disk> Disks { set; get; } = new List<Disk>(1);
        public byte OpenHandles { set; get; } = 0;
        public Stat Status { set; get; }
        public Dictionary<string, CacheEntry>? SubItems { set; get; }

        public CacheEntry(Disk d, Stat status)
        {
            Disks.Add(d);
            Status = status;
        }

        public ResolvedPath GetPhysicalPath(string virtualPath) => new ResolvedPath(Disks[0].PhysicalPath + virtualPath, Disks[0]);
    }
}
