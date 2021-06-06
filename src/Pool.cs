using Mono.Unix.Native;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hoardarr
{
    class Pool
    {
        readonly List<Disk> _disks;
        readonly Logger _logger;

        public List<Disk> Disks { get { return _disks; } }

        public Pool(IEnumerable<Disk> disks, Logger logger)
        {
            _logger = logger;
            _disks = disks.ToList();
            _ = Task.Factory.StartNew(() => PrintDiskStatsAsync(), TaskCreationOptions.LongRunning);
        }

        private async Task PrintDiskStatsAsync()
        {
            while (true)
            {
                await Task.Delay(60 * 1000 * 5);
                _logger.Warning("Disk stats: " + Environment.NewLine +  string.Join(Environment.NewLine, Disks.OrderBy(d=>d.DiskStats.f_bavail * d.DiskStats.f_bsize).Select(d => $"{d.PhysicalPath}:{(d.DiskStats.f_bavail * d.DiskStats.f_bsize) / (1024 * 1024 * 1024) }GB")));
            }
        }
    }
}