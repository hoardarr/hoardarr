using Mono.Fuse.NETStandard;
using Mono.Unix.Native;
using Serilog.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WyHash;

namespace Hoardarr
{
    class Cache
    {
		readonly Logger _logger;
		readonly Pool _pool;

		public CacheState State { set; get; } = CacheState.Starting;
		CacheEntry? _root = null;
		string _mountPoint;


		public Cache(Logger l, Pool pool, string mountPoint)
        {
			_mountPoint = mountPoint;
			_pool = pool;
			_logger = l;
			_ = Task.Factory.StartNew(() => BuildCache(pool), TaskCreationOptions.LongRunning);
		}

		public ResolvedCacheItem? GetItem(string virtualPath)
		{
			if (string.IsNullOrEmpty(virtualPath) || virtualPath == "/")
				return _root == null?null:new ResolvedCacheItem(_root, _mountPoint, virtualPath);
			var split = virtualPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
			var item = _root;

			foreach (var splitItem in split)
			{
				if (item == null)
					break;
				if (item.SubItems == null)
					item = null;
				else if (item.SubItems.TryGetValue(splitItem, out CacheEntry? i))
				{
					item = i;
				}
				else
					item = null;
			}

			return item==null?null:new ResolvedCacheItem(item, item.Disks[0].PhysicalPath + virtualPath, virtualPath);
		}

		public static FilePermissions FILE = FilePermissions.S_IROTH | FilePermissions.S_IRGRP | FilePermissions.S_IWUSR | FilePermissions.S_IRUSR | FilePermissions.S_IFREG;
		public static FilePermissions DIRECTORY = FilePermissions.S_IXOTH | FilePermissions.S_IROTH | FilePermissions.S_IXGRP | FilePermissions.S_IRGRP | FilePermissions.S_IRWXU | FilePermissions.S_IFDIR;

		public ResolvedCacheItem? Create(string virtualPath, Func<string, int> action, Disk? disk = null)
        {
			var split = virtualPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (_root == null)
            {
				_logger.Error($"Cache CreateDirectory failed as there is no root! [VPath:{virtualPath}]");
				return null;
			}

			var mostEmptyDisk = _pool.Disks.OrderByDescending(d => d.DiskStats.f_bfree * d.DiskStats.f_bsize).First();
			CacheEntry parent = _root;
			for (int i = 0; ; i++)
			{
				Monitor.Enter(parent);
					if (parent.SubItems == null)
						parent.SubItems = new Dictionary<string, CacheEntry>();

				if (i + 1 == split.Length)
				{
					// Perform create
					
						var bestParentDisk =  disk ?? parent.Disks.OrderByDescending(p => p.DiskStats.f_bfree * p.DiskStats.f_bsize).First();

					if (disk == null)
					{
						var freeBytes = bestParentDisk.DiskStats.f_bfree * bestParentDisk.DiskStats.f_frsize;
						var limit = (ulong)(75L * 1024 * 1024 * 1024);
						if (freeBytes < limit)
						{
							if (bestParentDisk != mostEmptyDisk)
							{
								Monitor.Exit(parent);
								_logger.Debug($"ACCESS Cache Create item low space disk swap [From:{parent.Disks[0].PhysicalPath}] [To:{mostEmptyDisk.PhysicalPath}]");
								if (!parent.Disks.Contains(mostEmptyDisk))
								{
									var parentOnNewDisk = Create("/" + string.Join('/', split.Take(split.Length - 1)), (string physicalPath) =>
									{
										int r = Syscall.mkdir(physicalPath, DIRECTORY);
										if (r != 0)
										{
											_logger.Error($"ACCESS Cache Create parent failed on different disk [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:{r}]");
											return -1;
										}

										return 0;
									}, mostEmptyDisk);

									if (parentOnNewDisk == null)
									{
										_logger.Error($"ACCESS Cache Create parent return failed on different disk [VPath:{virtualPath}]");
										return null;
									}
								}

								bestParentDisk = mostEmptyDisk;
								Monitor.Enter(parent);
							}
						}

					}

					var physicalPath = bestParentDisk.PhysicalPath + virtualPath; // TODO  targetDisk.PhysicalPath 
					if (action(physicalPath) == -1)
					{
						Monitor.Exit(parent);
						_logger.Error($"ACCESS Cache Create item [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:false]");
						return null;
					}

					if (Syscall.lstat(physicalPath, out Stat newStatusStats) == -1)
					{
						Monitor.Exit(parent);
						_logger.Error($"ACCESS Cache Create item stats failed [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:{Syscall.GetLastError()}] [Result:false]");
						return null;
					}

					newStatusStats.CleanStat(virtualPath);

					if (parent.SubItems.TryGetValue(split[i], out CacheEntry? existingValue))
					{
						_logger.Debug($"ACCESS Cache Create append existing [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:true] [Inode:{newStatusStats.st_ino}]");
						lock (existingValue)
						{
							existingValue.Disks = new List<Disk>() { bestParentDisk }.Concat(existingValue.Disks).ToList();
							existingValue.Status = newStatusStats;
						}
						Monitor.Exit(parent);
						return new ResolvedCacheItem(existingValue, physicalPath, virtualPath);
					}
					else
					{
						_logger.Debug($"ACCESS Cache Create item [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:true] [Inode:{newStatusStats.st_ino}]");
						var item = new CacheEntry(bestParentDisk, newStatusStats);
						parent.SubItems.Add(split.Last(), item);
						Monitor.Exit(parent);
						return new ResolvedCacheItem(item, physicalPath, virtualPath);
					}
				}
				else if (parent.SubItems.TryGetValue(split[i], out CacheEntry? subValue) && subValue != null)
				{
					Monitor.Exit(parent);
					parent = subValue;

					Monitor.Enter(subValue);
					if (disk != null && !subValue.Disks.Contains(disk))
					{
						var parentVirtualPath = "/" + string.Join('/', split.Take(i + 1));
						var physicalPath = disk.PhysicalPath + parentVirtualPath;
						int r = Syscall.mkdir(physicalPath, DIRECTORY);
						if (r != 0)
						{
							Monitor.Exit(subValue);
							_logger.Error($"ACCESS Cache Create parent alt drive [VPath:{parentVirtualPath}] [PhysPath:{physicalPath}] [Result:{r}]");
							return null;
						}
					}

					Monitor.Exit(subValue);
				}
				else
                {
					var bestParentDisk = parent.Disks.OrderByDescending(p => p.DiskStats.f_bfree * p.DiskStats.f_bsize).First();
					var physicalPath = bestParentDisk.PhysicalPath + "/" + string.Join('/', split.Take(i + 1));

					int r = Syscall.mkdir(physicalPath, DIRECTORY);
					if (r != 0)
					{
						Monitor.Exit(parent);
						_logger.Error($"ACCESS Cache Create parent [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:{r}]");
						return null;
					}

					if (Syscall.lstat(physicalPath, out Stat newStatusStats) == -1)
					{
						Monitor.Exit(parent);
						_logger.Error($"ACCESS Cache Create parent stats failed [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:{r}]");
						return null;
					}

					if (Syscall.close(r) != 0)
					{
						_logger.Error($"ACCESS Cache Create closefailed [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:{Syscall.GetLastError()}]");
					}
					newStatusStats.CleanStat(virtualPath);
					var item = new CacheEntry(bestParentDisk, newStatusStats);
					parent.SubItems.Add(split.Last(), item);
					Monitor.Exit(parent);
					parent = item;
				}
			}
		}


		public void Remove(string path)
		{
            if (_root == null)
            {
				_logger.Error($"Cache failed to remove as there is no root! [VPath:{path}]");
				return;
            }
			var parentDir = Path.GetDirectoryName(path);
			var parent = parentDir == null ? new ResolvedCacheItem(_root, _root.Disks[0].PhysicalPath, "/"): GetItem(parentDir);
			if (parent == null || parent.Entry.SubItems == null)
            {
				_logger.Error($"Cache failed to remove [VPath:{path}]");
				return;
            }
			var name = Path.GetFileName(path);
			lock (parent)
				parent.Entry.SubItems.Remove(name);
		}

		private async Task BuildCache(Pool pool)
		{
			/*  for(int i=0;i<60;i++)
			  {
				  await Task.Delay(1000);
			  }*/
			await Task.CompletedTask;

			foreach (var disk in pool.Disks)
			{
				_logger.Warning($"Cache scan from {disk.PhysicalPath}");
				if (Syscall.lstat(disk.PhysicalPath, out Stat newStatusStats) == -1)
				{
					_logger.Error($"Cache lstat failed: {disk.PhysicalPath}");
					continue;
				}

				if(_root== null)
                {
					newStatusStats.CleanStat("/");
					_root = new CacheEntry(disk, newStatusStats);
                }
				else if(_root.Status.st_mtime< newStatusStats.st_mtime)
                {
					newStatusStats.CleanStat("/");
					_root.Status = newStatusStats;
                }

				IntPtr dp = Syscall.opendir(disk.PhysicalPath);
				if (dp == IntPtr.Zero)
				{
					_logger.Error($"Cache opendir failed: {disk.PhysicalPath}");
					continue;
				}
				Dirent de;
				while ((de = Syscall.readdir(dp)) != null)
				{
					if (de.d_name == ".." || de.d_name == ".")
						continue;
					CacheItem("/" + de.d_name, disk, _root);
				}

				Syscall.closedir(dp);
			}

			State = CacheState.Active;
			_logger.Warning($"Cache active!");
		}

		private CacheEntry? CacheItem(string path, Disk disk, CacheEntry parent, bool processChildren = true)
		{
			var physicalpath = disk.PhysicalPath +path;
			var name = Path.GetFileName(path);
			if (Syscall.lstat(physicalpath, out Stat newStatusStats) == -1)
			{
				lock (parent)
				{
					if (parent.SubItems != null && parent.SubItems.ContainsKey(name))
					{
						parent.SubItems.Remove(name);
					}
				}
				return null;
			}
			 newStatusStats.CleanStat(path);

			if (parent.SubItems == null)
				parent.SubItems = new Dictionary<string, CacheEntry>();

			CacheEntry currentItem;
			if (parent.SubItems.ContainsKey(name))
			{
				currentItem = parent.SubItems.First(f => f.Key == name).Value;
				if (currentItem.Disks[0] != disk)
					currentItem.Disks = (new List<Disk>() { disk }.Concat(currentItem.Disks).Distinct().ToList());
				currentItem.Disks.Add(disk);
				if (currentItem.Status.st_mtime <= newStatusStats.st_mtime)
				{
					// newer mod date
					currentItem.Status = newStatusStats;
				}
			}
			else
			{
				currentItem = new CacheEntry(disk, newStatusStats);
				parent.SubItems.Add(name, currentItem);
			}

			if (processChildren && ((FilePermissions)newStatusStats.st_mode & FilePermissions.S_IFDIR) == FilePermissions.S_IFDIR)
			{
				// Item is a directory - cache its sub items
				IntPtr dp = Syscall.opendir(physicalpath);
				if (dp == IntPtr.Zero)
				{
					_logger.Error($"Failed to cache path: {physicalpath}");
					return currentItem;
				}
				Dirent de;
				while ((de = Syscall.readdir(dp)) !=null)
				{
					if (de.d_name == ".." || de.d_name == ".")
						continue;
					CacheItem(path+ "/" + de.d_name, disk, currentItem);
				}
				Syscall.closedir(dp);
			}

			return currentItem;
		}
    }

    public enum CacheState { Starting, Active }

	public class UpdatedPath
    {
		public string Path { set; get; }
		public Disk Disk { set; get; }
		public bool IncrementHandle { set; get; }

		public UpdatedPath(string path, Disk disk, bool incrementHandle)
        {
			Path = path;
			Disk = disk;
			IncrementHandle = incrementHandle;
        }
    }
}
