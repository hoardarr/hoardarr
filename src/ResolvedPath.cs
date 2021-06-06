using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hoardarr
{
    public record ResolvedPath (string physicalPath, Disk disk);
}
