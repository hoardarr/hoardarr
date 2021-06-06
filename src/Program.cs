using Serilog;
using System;
using System.Diagnostics;
using System.IO;

namespace Hoardarr
{
    class Program
    {
        static void Main(string[] args)
        {
            using var log = new LoggerConfiguration()
                .MinimumLevel.Verbose()
   // .WriteTo.File(Path.Combine(AppContext.BaseDirectory, $"{Process.GetCurrentProcess().Id}.log"))
    .WriteTo.Console(Serilog.Events.LogEventLevel.Warning)
    .CreateLogger();
            using var fs = new PoolFileSystem(log, args);
            fs.ParseFuseArguments("-o allow_other -o auto_unmount -o large_read -o big_writes -o default_permissions -o kernel_cache -o auto_cache -o use_ino".Split(' '));  //  -
            fs.Start();
        }
    }
}
