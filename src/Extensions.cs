using Mono.Unix.Native;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WyHash;

namespace Hoardarr
{
    public static class Extensions
    {

        public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T?> e) where T : class
        {
            return e.Where(e => e != null)!;
        }

        public static void CleanStat(this Stat stat, string path)
        {
            stat.st_ino = WyHash64.ComputeHash64(Encoding.UTF8.GetBytes(path));
            stat.st_dev = 0;
            stat.st_rdev = 0;
            stat.st_nlink = 1;
        }
    }
}
