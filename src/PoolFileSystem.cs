using Mono.Fuse.NETStandard;
using Mono.Unix.Native;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using WyHash;

namespace Hoardarr
{
    public class PoolFileSystem: FileSystem
    {
		readonly Logger _logger;
		readonly Pool _pool;
		readonly Cache _cache;

		readonly Regex _regex = new Regex(@"(?<blk>(sd)[a-z]{0,5}).*\n.*\n.*(?<mnt>/mnt.*)", RegexOptions.Compiled);

		public PoolFileSystem(Logger logger, string[] dirs)
		{
			_logger = logger;
			MountPoint = dirs[0];
		
			// Create pool checking each disk
			var disks = dirs.Skip(1).SelectMany(d =>
		{
				// Expand asterisk
				if (d.Contains("*"))
			{
				if (System.IO.Path.GetFileName(d) == "*")
					return System.IO.Directory.GetDirectories(System.IO.Path.GetDirectoryName(d)!);
				return System.IO.Directory.GetDirectories(d);
			}
			return new string[] { d };
		}).Select(path =>
		{
			// Check access
			try
			{
				System.IO.Directory.GetFiles(path);
			}
			catch (Exception)
			{
				logger.Error($"Root directory not accessible: {path}");
				throw;
			}
			// Folder stats
			if (Syscall.lstat(path, out Stat newMountStats) == -1)
			{
				throw new Exception("Failed to lstat root " + path);
			}
			newMountStats.CleanStat(MountPoint);
			// Disk stats
			if (Syscall.statvfs(path, out Statvfs newDiskStats) == -1)
			{
				throw new Exception("Failed to statvfs root " + path);
			}

			var disk = new Disk(path, newMountStats, newDiskStats);
			return disk;
		});
			_pool = new Pool(disks, logger);
			_cache = new Cache(logger, _pool, MountPoint);

			logger.Warning($"Mount: {MountPoint} Sources: {string.Join(", ", _pool.Disks.Select(s => $"{s.PhysicalPath}"))}");
		}

		/*
		/// <summary>
		/// Utility to expand a virtual path to a phsysical based on cached if its active else fallback to checking disks
		/// </summary>
		/// <param name="path"></param>
		/// <param name="memberName"></param>
		/// <returns></returns>
		private Tuple<string, Disk>? GetPhysicalPath(string path, bool incrementHandles,  [System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
		{
			if (_cache.State == CacheState.Active)
			{
				var cacheItem = _cache.GetItem(path);
				if (cacheItem == null)
				{
					_logger.Debug($"{memberName} Failed - {path}");
					return null;
				}

                if (incrementHandles)
                {
					lock (cacheItem)
						cacheItem.OpenHandles++;
                }

				return new Tuple<string, Disk>(cacheItem.Disks[0].PhysicalPath+path, cacheItem.Disks[0]);
			}
			else
			{
				var physicalPath = _pool.GetFullPath(path);
				_logger.Debug($"ACCESS {memberName} - {path} -> {physicalPath}");
				return physicalPath;
			}
		}

		public ResolvedPath GetNewPath(string virtualPath)
		{
			if (_cache.State == CacheState.Active)
			{
				var cacheItem = _cache.GetItem(virtualPath);
                if (cacheItem != null)
                {
					return cacheItem.GetPhysicalPath(virtualPath);
                }



			}
			else
            {
				return _pool.GetNewPath(virtualPath);
            }
		}*/

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void WaitForCache()
		{
			// This is probably a bad way of doing this!
			while (_cache.State != CacheState.Active)
				Thread.Sleep(100);
		}

		protected override Errno OnGetPathStatus(string virtualPath, out Stat buf)
		{
			if (string.IsNullOrEmpty(virtualPath) || virtualPath == "/")
			{
				buf = _pool.Disks[0].MountStats;

				foreach (var disk in _pool.Disks.Skip(1))
				{
					buf.st_size += disk.MountStats.st_size;
					buf.st_blocks += disk.MountStats.st_blocks;
				}

				return 0;
			}
			else
			{
				WaitForCache();
				var cacheItem = _cache.GetItem(virtualPath);
				if (cacheItem == null)
				{
					_logger.Debug($"Failed OnGetPathStatus [Path:{virtualPath}]");
					buf = new Stat();
					return Errno.ENOENT;
				}
				else
				{
					if (cacheItem.Entry.OpenHandles == 0)
					{
						buf = cacheItem.Entry.Status;
						_logger.Debug($"OnGetPathStatus [Path:{virtualPath}] [Inode:{buf.st_ino}] [Type:{buf.st_mode}]");
						return 0;
					}
					else
					{
						int result = Syscall.lstat(cacheItem.PhysicalPath, out buf);
						buf.CleanStat(virtualPath);
						_logger.Information($"ACCESS OnGetPathStatus [Handles:{cacheItem.Entry.OpenHandles}] [VPath:{virtualPath}] [Inode:{buf.st_ino}] [Result:{result}]");
						cacheItem.Entry.Disks[0].Touch();
						return LogAnyError(virtualPath, result==-1?-1:0);
					}
				}

			}
		}

		protected override Errno OnGetHandleStatus(string virtualPath, OpenedPathInfo info, out Stat buf)
		{
			int r = Syscall.fstat((int)info.Handle, out buf);
			buf.CleanStat(virtualPath);
			if (virtualPath == "/")
				_logger.Debug($"OnGetHandleStatus [VPath:{virtualPath}] [Inode:{buf.st_ino}] [Result:{r}]");
			else
				_logger.Information($"ACCESS OnGetHandleStatus [VPath:{virtualPath}] [Inode:{buf.st_ino}] [Result:{r}]");
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnAccessPath(string virtualPath, AccessModes mask)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
				return LogAnyError(virtualPath, Errno.ENOENT);
			int r = Syscall.access(cacheItem.PhysicalPath, mask);
			_logger.Information($"ACCESS OnAccessPath Cache [VPath:{virtualPath}] [Path:{cacheItem.PhysicalPath}] [Access:{mask}] [Result:{r}]");
			cacheItem.Entry.Disks[0].Touch();
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnReadSymbolicLink(string virtualPath, out string? target)
		{
			target = null;
			return LogAnyError(virtualPath, Errno.ENOENT);
		}

		protected override Errno OnOpenDirectory(string virtualPath, OpenedPathInfo info)
		{
			var isCreate = ((info.OpenFlags & OpenFlags.O_CREAT) == OpenFlags.O_CREAT);
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				if (isCreate)
				{
					 cacheItem = _cache.Create(virtualPath, (string physicalPath) =>
					{
						var result = Syscall.mkdir(physicalPath,Cache.DIRECTORY);
						return result;
					});
				}
				if(cacheItem == null)
					return LogAnyError(virtualPath, Errno.ENOENT);
				cacheItem.UpdateStats(_logger);
			}

			info.Handle = new IntPtr((int)(cacheItem.Entry.Status.st_ino % int.MaxValue));
			_logger.Debug($"OnOpenDirectory [VPath:{virtualPath}] [Path:{cacheItem.PhysicalPath}]");
			return 0;
		}

		protected override Errno OnReadDirectory(string path, OpenedPathInfo fi,
				out List<DirectoryEntry> paths)
		{
			WaitForCache();
			var cachedItem = _cache.GetItem(path);
			if (cachedItem == null)
			{
				_logger.Debug($"OnReadDirectory [VPath:{path}] [Result:Not Found!]");
				paths = new List<DirectoryEntry>();
				return Errno.ENOENT;
			}
			else
			{
				_logger.Debug($"OnReadDirectory [Path:{path}]");
				var entry = cachedItem.Entry;
				var items = new List<DirectoryEntry>(entry.SubItems?.Count ?? 0);
				lock (entry)
				{
					if (entry.SubItems != null)
					{
						foreach (var item in entry.SubItems)
						{
							items.Add(new DirectoryEntry(item.Key)
							{
								Stat = item.Value.Status
							});
							_logger.Verbose($"OnReadDirectory [Path:{path}] [Target:{path}/{item.Key}] [INode:{item.Value.Status.st_ino}] [Mode:{item.Value.Status.st_mode}]");
						}
					}
				}
				paths = items;
				_logger.Debug($"OnReadDirectory [VPath:{path}] [Result:{paths.Count()}]");
				return 0;
			}
		}

		protected override Errno OnReleaseDirectory(string virtualPath, OpenedPathInfo info)
		{
			WaitForCache();

			/*
			if(virtualPath == "-")
            {
				IntPtr dp = info.Handle;
				var r = Syscall.closedir(dp);
				_logger.Debug($"OnReleaseDirectory deleted [VPath:{virtualPath}] [Result:{r}]");
				// Deleted ignore
				return (Errno)r;
            }
            else
            {
				var cacheItem = _cache.GetItem(virtualPath);
                if (cacheItem == null)
                {
					_logger.Error($"OnReleaseDirectory not found [VPath:{virtualPath}]");
					return LogAnyError(virtualPath, Errno.ENOENT);
				}

				if (cacheItem.Entry.OpenHandles > 0)
				{
					IntPtr dp = info.Handle;
					var r = Syscall.closedir(dp);
					if(r==-1)
                    {
						var result = Syscall.GetLastError();
						_logger.Error($"OnReleaseDirectory [VPath:{virtualPath}] [Handles:{cacheItem.Entry.OpenHandles}] [Result:{result}");
						return result;
					}
					lock (cacheItem.Entry)
						cacheItem.Entry.OpenHandles--;
				}
				_logger.Information($"OnReleaseDirectory [VPath:{virtualPath}] [Handles:{cacheItem.Entry.OpenHandles}]");*/

			return 0;
		}

		protected override Errno OnCreateSpecialFile(string virtualPath, FilePermissions mode, ulong rdev)
		{
			WaitForCache();

			Func<string, int> action  = (string physicalPath) =>
			 {
				 int r;

				 // On Linux, this could just be `mknod(basedir+path, mode, rdev)' but 
				 // this is more portable.
				 if ((mode & FilePermissions.S_IFMT) == FilePermissions.S_IFREG)
				 {
					 r = Syscall.open(physicalPath, OpenFlags.O_CREAT | OpenFlags.O_EXCL |
							 OpenFlags.O_WRONLY, mode);
					 if (r >= 0)
						 r = Syscall.close(r);
				 }
				 else if ((mode & FilePermissions.S_IFMT) == FilePermissions.S_IFIFO)
				 {
					 r = Syscall.mkfifo(physicalPath, mode);
				 }
				 else
				 {
					 r = Syscall.mknod(physicalPath, mode, rdev);
				 }

				 // TODO Permissions?
				 if (r != -1)
				 {
					 var context = GetOperationContext();
					 r = Syscall.chown(physicalPath, (uint)context.UserId, (uint)context.GroupId);
				 }

				 _logger.Information($"OnCreateSpecialFile [VPath:{virtualPath}] [PhysPath:{physicalPath}] [Result:{r}]");
				 return r;
			 };

			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				cacheItem = _cache.Create(virtualPath, action);
				if (cacheItem == null)
					return LogAnyError(virtualPath, Errno.ENOENT);
				cacheItem.UpdateStats(_logger);
				return 0;
			}

			var result =  action(cacheItem.PhysicalPath);
			return LogAnyError(virtualPath, result);
		}

		protected override Errno OnCreateDirectory(string virtualPath, FilePermissions mode)
		{
			WaitForCache();

			Func<string, int> action = (string physicalPath) =>
			{
				int r = Syscall.mkdir(physicalPath, mode);
				_logger.Information($"ACCESS OnCreateDirectory [VPath:{virtualPath}] [Path:{physicalPath}] [Result:{r}]");
				if (r == -1)
					return r;
				else
				{
					var context = GetOperationContext();
					return Syscall.chown(physicalPath, (uint)context.UserId, (uint)context.GroupId);
				}
			};
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				cacheItem = _cache.Create(virtualPath, action);
				if (cacheItem == null)
					return LogAnyError(virtualPath, Errno.ENOENT);
				return 0;
			}

			var result = action(cacheItem.PhysicalPath);
			if (result != -1)
				cacheItem.UpdateStats(_logger);
			return LogAnyError(virtualPath, result);
		}

		protected override Errno OnRemoveFile(string virtualPath)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				return LogAnyError(virtualPath, Errno.ENOENT);
			}

			bool ok = false;
			int lastResult = 0;
			foreach(var disk in cacheItem.Entry.Disks)
            {
				disk.Touch();
				var physicalPath = disk.PhysicalPath + virtualPath;
				lastResult = Syscall.unlink(disk.PhysicalPath + virtualPath);
				if (lastResult != -1)
				{
					ok = true;
					_logger.Information($"ACCESS OnRemoveFile [VPath:{virtualPath}] [Path:{physicalPath}] [Result:{lastResult}] [Handles:{cacheItem.Entry.OpenHandles}]");
				}
			}

            if (ok)
            {
				_cache.Remove(virtualPath);
				return 0;
			}

			return LogAnyError(virtualPath, Errno.ENOENT);
		}

		protected override Errno OnRemoveDirectory(string virtualPath)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				return LogAnyError(virtualPath, Errno.ENOENT);
			}

			bool ok = false;
			int lastResult = 0;
			foreach (var disk in cacheItem.Entry.Disks)
			{
				disk.Touch();
				var physicalPath = disk.PhysicalPath + virtualPath;
				lastResult = Syscall.rmdir(disk.PhysicalPath + virtualPath);
				if (lastResult != -1)
				{
					ok = true;
					_logger.Information($"ACCESS OnRemoveDirectory [VPath:{virtualPath}] [Path:{physicalPath}] [Result:{lastResult}] [Handles:{cacheItem.Entry.OpenHandles}]");
				}
			}

			if (ok)
			{
				_cache.Remove(virtualPath);
				return 0;
			}

			return LogAnyError(virtualPath, Errno.ENOENT);
		}

		protected override Errno OnCreateSymbolicLink(string from, string to)
		{
			return Errno.EOPNOTSUPP;
		}

		protected override Errno OnRenamePath(string fromVirtualPath, string toVirtualPath)
		{
			WaitForCache();

			var fromCacheItem = _cache.GetItem(fromVirtualPath);
			if (fromCacheItem == null)
			{
				_logger.Error($"OnRenamePath not found! [VPathFrom:{fromVirtualPath}]");
				return Errno.ENOENT;
			}

			var isDir = Directory.Exists(fromCacheItem.PhysicalPath);
			Func<string, int> create = (string physicalPath) =>
			{
				if (isDir)
				{
					return Syscall.mkdir(physicalPath, Cache.DIRECTORY);
				}
				else
				{
					return Syscall.mknod(physicalPath, Cache.FILE, 0);
				}
			};


			var toCacheItem = _cache.GetItem(toVirtualPath);
			if (toCacheItem == null)
			{
				toCacheItem = _cache.Create(toVirtualPath, create, fromCacheItem.Entry.Disks[0]);
				if (toCacheItem == null)
					return LogAnyError(toVirtualPath, Errno.ENOENT);
			}

			if (toCacheItem == null)
			{
				_logger.Error($"OnRenamePath destination not found! [VPathFrom:{fromVirtualPath}] [VPathTo:{toVirtualPath}]");
				return Errno.ENOENT;
			}

			var result = Syscall.rename(fromCacheItem.PhysicalPath, toCacheItem.PhysicalPath);
			_logger.Information($"OnRenamePath rename file same disk! [VPathFrom:{fromVirtualPath}] [VPathTo:{toVirtualPath}] [PhysPathFrom:{ fromCacheItem.PhysicalPath}] [PhysPathTo:{toCacheItem.PhysicalPath}] [Result:{result}]");
			if (result != -1)
			{
                if (isDir)
                {
					toCacheItem.Entry.SubItems = fromCacheItem.Entry.SubItems;


                }
				// TODO : Different files in a folder on different disks
				_cache.Remove(fromVirtualPath);
				toCacheItem.UpdateStats(_logger);
			}
			return LogAnyError(toVirtualPath, result);
		}

		Errno LogAnyError(string virtualPath, int e, [System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
		{
			if (e == -1)
			{
				var error = Stdlib.GetLastError();
				_logger.Warning($"{memberName} [VPath:{virtualPath}] [Result:{error}]");
				return error;
			}
			return 0;
		}

		Errno LogAnyError(string virtualPath, Errno e, [System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
		{
			_logger.Warning($"{memberName} [VPath:{virtualPath}] [Result:{e}]");
			return e;
		}
		

		protected override Errno OnCreateHardLink(string from, string to)
		{
			return Errno.EOPNOTSUPP;
		}

		protected override Errno OnChangePathPermissions(string virtualPath, FilePermissions mode)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				_logger.Error($"OnChangePathPermissions not found! {virtualPath}");
				return Errno.ENOENT;
			}

			int r = Syscall.chmod(cacheItem.PhysicalPath, mode);
			_logger.Information($"ACCESS OnChangePathPermissions [VPath:{virtualPath}]  [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			cacheItem.UpdateStats(_logger);
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnChangePathOwner(string virtualPath, long uid, long gid)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				_logger.Error($"OnChangePathOwner not found! {virtualPath}");
				return Errno.ENOENT;
			}

			int r = Syscall.lchown(cacheItem.PhysicalPath, (uint)uid, (uint)gid);
			_logger.Information($"ACCESS OnChangePathOwner [VPath:{virtualPath}]  [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			cacheItem.UpdateStats(_logger);
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnTruncateFile(string virtualPath, long size)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				_logger.Error($"OnTruncateFile not found! {virtualPath}");
				return Errno.ENOENT;
			}

			int r = Syscall.truncate(cacheItem.PhysicalPath, size);
			_logger.Information($"ACCESS OnTruncateFile [VPath:{virtualPath}]  [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			cacheItem.UpdateStats(_logger);
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnTruncateHandle(string virtualPath, OpenedPathInfo info, long size)
		{
			WaitForCache();
			int r = Syscall.ftruncate((int)info.Handle, size);
			_logger.Information($"ACCESS OnTruncateHandle [VPath:{virtualPath}] [Result:{r}]");
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem != null)
				cacheItem.UpdateStats(_logger);
			else
				_logger.Error($"OnTruncateHandle Cache item not found! [VPath:{virtualPath}]");
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnChangePathTimes(string virtualPath, ref Utimbuf buf)
		{
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
            {
				_logger.Error($"OnChangePathTimes not found! {virtualPath}");
				return Errno.ENOENT;
			}

			int r = Syscall.utime(cacheItem.PhysicalPath, ref buf);
			_logger.Information($"ACCESS OnChangePathTimes [VPath:{virtualPath}]  [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			cacheItem.UpdateStats(_logger);
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnCreateHandle(string virtualPath, OpenedPathInfo info, FilePermissions mode)
		{
			WaitForCache();
			var isCreate = ((info.OpenFlags & OpenFlags.O_CREAT) == OpenFlags.O_CREAT);
			Func<string, int> action = (string physicalPath) =>
			{
				int fd = Syscall.open(physicalPath, info.OpenFlags, mode);
				if (fd == -1)
					return fd;
				info.Handle = (IntPtr)fd;
				_logger.Information($"ACCESS OnCreateHandle [VPath:{virtualPath}]  [PhysPath:{physicalPath}] [Create:{isCreate}] [Result:{fd}]");

				if (isCreate)
				{
					var context = GetOperationContext();
					return Syscall.chown(physicalPath, (uint)context.UserId, (uint)context.GroupId);

				};

				return 0;
			};
			var cacheItem = _cache.GetItem(virtualPath);

			if (cacheItem == null)
			{
				if (isCreate)
				{
					cacheItem = _cache.Create(virtualPath, action);
					if (cacheItem != null)
					{
						cacheItem.UpdateStats(_logger);
						lock (cacheItem.Entry)
							cacheItem.Entry.OpenHandles++;
						return 0;
					}
				}

				return LogAnyError(virtualPath, Errno.ENOENT);
			}


			var result = action(cacheItem.PhysicalPath);
			if (result != -1)
			{
				cacheItem.UpdateStats(_logger);
				lock (cacheItem.Entry)
					cacheItem.Entry.OpenHandles++;
			}
			return LogAnyError(virtualPath, result);
		}

		protected override Errno OnOpenHandle(string virtualPath, OpenedPathInfo info)
		{
			WaitForCache();
			var isCreate = ((info.OpenFlags & OpenFlags.O_CREAT) == OpenFlags.O_CREAT);

			var cacheItem = _cache.GetItem(virtualPath);
			Func<string, int> action = (string physicalPath) =>
			 {

				 int r = Syscall.open(physicalPath, info.OpenFlags);
				 
				 info.Handle = (IntPtr)r;
				 _logger.Information($"ACCESS OnOpenHandle [VPath:{virtualPath}]  [PhysPath:{physicalPath}] [Result:{r}]");
				 if (isCreate && r != -1)
				 {
					 // Todo: permissions?
					 var context = GetOperationContext();
					 r = Syscall.chown(physicalPath, (uint)context.UserId, (uint)context.GroupId);
					 _logger.Information($"ACCESS OnOpenHandle Create Permissions [VPath:{virtualPath}]  [PhysPath:{physicalPath}] [Result:{r}]");
				 }
				 return r== -1?-1:0;
			 };

			if (cacheItem == null)
			{
				if (isCreate)
				{
					cacheItem = _cache.Create(virtualPath, action);
					if (cacheItem != null)
					{
						lock (cacheItem.Entry)
						{
							cacheItem.Entry.OpenHandles++;
						}
						return 0;
					}
				}

				if (cacheItem == null)
					return LogAnyError(virtualPath, Errno.ENOENT);
			}

			var result = action(cacheItem.PhysicalPath);
			if (result!= -1)
			{
				lock (cacheItem.Entry)
				{
					cacheItem.Entry.OpenHandles++;
				}
				cacheItem.UpdateStats(_logger);
			}
			return LogAnyError(virtualPath, result);
		}

		protected override unsafe Errno OnReadHandle(string virtualPath, OpenedPathInfo info, byte[] buf,
				long offset, out int bytesRead)
		{
			int r;
			fixed (byte* pb = buf)
			{
				r = bytesRead = (int)Syscall.pread((int)info.Handle,
						pb, (ulong)buf.Length, offset);
			}

			_logger.Verbose($"OnReadHandle [VPath:{virtualPath}] [Handle:{info.Handle}] [Result:{r}]");

			return LogAnyError(virtualPath, r);
		}

		protected override unsafe Errno OnWriteHandle(string virtualPath, OpenedPathInfo info,
				byte[] buf, long offset, out int bytesWritten)
		{
			int r;
			fixed (byte* pb = buf)
			{
				r = bytesWritten = (int)Syscall.pwrite((int)info.Handle,
						pb, (ulong)buf.Length, offset);
			}
			_logger.Verbose($"OnWriteHandle [VPath:{virtualPath}] [Handle:{info.Handle}] [Result:{r}]");
			return LogAnyError(virtualPath, r == -1?-1:0);
		}

		protected override Errno OnGetFileSystemStatus(string virtualPath, out Statvfs stbuf)
		{
			_logger.Debug($"OnGetFileSystemStatus [VPath:{virtualPath}]");

			stbuf = _pool.Disks[0].DiskStats;
			foreach (var disk in _pool.Disks.Skip(1))
			{
				stbuf.f_bavail += ((disk.DiskStats.f_bavail * disk.DiskStats.f_frsize) / stbuf.f_frsize);
				stbuf.f_bfree += ((disk.DiskStats.f_bfree * disk.DiskStats.f_frsize) / stbuf.f_frsize);
				stbuf.f_blocks += ((disk.DiskStats.f_blocks * disk.DiskStats.f_frsize) / stbuf.f_frsize);
				stbuf.f_bsize += ((disk.DiskStats.f_bsize * disk.DiskStats.f_frsize) / stbuf.f_frsize);
				stbuf.f_favail += disk.DiskStats.f_favail;
				stbuf.f_ffree += disk.DiskStats.f_ffree;
				stbuf.f_files += disk.DiskStats.f_files;
			}

			return 0;
		}

		protected override Errno OnFlushHandle(string virtualPath, OpenedPathInfo info)
		{
			//return 0;
			_logger.Debug($"OnFlushHandle - {virtualPath}");
			/* This is called from every close on an open file, so call the
			   close on the underlying filesystem.  But since flush may be
			   called multiple times for an open file, this must not really
			   close the file.  This is important if used on a network
			   filesystem like NFS which flush the data/metadata on close() */
			int r = Syscall.close(Syscall.dup((int)info.Handle));

			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnReleaseHandle(string virtualPath, OpenedPathInfo info)
		{
			int r = Syscall.close((int)info.Handle);

			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem != null)
			{
				lock(cacheItem.Entry)
				  cacheItem.Entry.OpenHandles--;
				_logger.Information($"OnReleaseHandle [VPath:{virtualPath}] [Handles:{cacheItem.Entry.OpenHandles}] [Result:{r}]");
				cacheItem.UpdateStats(_logger);
				cacheItem.Entry.Disks[0].UpdateDiskStats(_logger);
			}
			else
				_logger.Error($"OnReleaseHandle no entry [VPath:{virtualPath}] [Result:{r}]");
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnSynchronizeHandle(string virtualPath, OpenedPathInfo info, bool onlyUserData)
		{
			WaitForCache();
			int r;
			if (onlyUserData)
				r = Syscall.fdatasync((int)info.Handle);
			else
				r = Syscall.fsync((int)info.Handle);
			_logger.Verbose($"ACCESS OnSynchronizeHandle [VPath:{virtualPath}] [Result:{r}]");
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem != null)
				cacheItem.UpdateStats(_logger);
			else
				_logger.Error($"OnSynchronizeHandle no entry [VPath:{virtualPath}] [Result:{r}]");
			return LogAnyError(virtualPath, r);
		}

		protected override Errno OnSetPathExtendedAttribute(string virtualPath, string name, byte[] value, XattrFlags flags)
		{
			return Errno.EOPNOTSUPP;
			/*WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
            if (cacheItem == null)
				return LogAnyError(virtualPath, Errno.ENOENT);
			int r = Syscall.lsetxattr(cacheItem.PhysicalPath, name, value, (ulong)value.Length, flags);
			_logger.Information($"ACCESS OnSetPathExtendedAttribute [VPath:{virtualPath}] [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			cacheItem.UpdateStats(_logger);
			return LogAnyError(virtualPath, r);*/
		}

		protected override Errno OnGetPathExtendedAttribute(string virtualPath, string name, byte[] value, out int bytesWritten)
		{
			bytesWritten = 0;
			return Errno.EOPNOTSUPP;
			/*
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				bytesWritten = 0;
				return LogAnyError(virtualPath, Errno.ENOENT);
			}

			bytesWritten = (int)Syscall.lgetxattr(cacheItem.PhysicalPath, name, value, (ulong)(value?.Length ?? 0));

			_logger.Debug($"ACCESS OnGetPathExtendedAttribute [VPath:{virtualPath}] [PhysPath:{cacheItem.PhysicalPath}] [Result:{bytesWritten}]");

			if (bytesWritten == -1)
			{
				var lastError = Stdlib.GetLastError();
				bytesWritten = 0;
				if (lastError == Errno.ENODATA)
					return lastError;
				return LogAnyError(virtualPath, lastError);
			}

			return 0;*/
		}

		protected override Errno OnListPathExtendedAttributes(string virtualPath, out string[] names)
		{
			names = new string[0];
			return Errno.EOPNOTSUPP;
			/*
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				names = new string[0];
				return LogAnyError(virtualPath, Errno.ENOENT);
			}

			int r = (int)Syscall.llistxattr(cacheItem.PhysicalPath, out names);
			_logger.Information($"ACCESS OnListPathExtendedAttributes [VPath:{virtualPath}] [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			return LogAnyError(virtualPath, r);*/
		}

		protected override Errno OnRemovePathExtendedAttribute(string virtualPath, string name)
		{
			return Errno.EOPNOTSUPP;
			/*
			WaitForCache();
			var cacheItem = _cache.GetItem(virtualPath);
			if (cacheItem == null)
			{
				return LogAnyError(virtualPath, Errno.ENOENT);
			}

			int r = Syscall.lremovexattr(cacheItem.PhysicalPath, name);
			_logger.Information($"ACCESS OnRemovePathExtendedAttribute [VPath:{virtualPath}] [PhysPath:{cacheItem.PhysicalPath}] [Result:{r}]");
			return LogAnyError(virtualPath, r);*/
		}

		protected override Errno OnLockHandle(string virtualPath, OpenedPathInfo info, FcntlCommand cmd, ref Flock @lock)
		{
			_logger.Verbose($"OnLockHandle [VPath:{virtualPath}]");
			int r = Syscall.fcntl((int)info.Handle, cmd, ref @lock);
			return LogAnyError(virtualPath, r);
		}
	}
}
