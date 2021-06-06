using Mono.Unix.Native;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hoardarr
{
    public record ResolvedCacheItem(CacheEntry Entry, string PhysicalPath, string VirtualPath)
    {
        public void UpdateStats(Logger logger)
        {
            lock (Entry)
            {
                if (Syscall.lstat(PhysicalPath, out Stat newStatusStats) != 0)
                {
                    logger.Error($"Failed to update stats for {PhysicalPath}");
                }
                else
                {
                    newStatusStats.CleanStat(VirtualPath);
                    Entry.Status = newStatusStats;
                }
            }

            Entry.Disks[0].Touch();
        }
    }
}
