use std::cmp::max;
use std::ffi::{OsStr, OsString};
use std::result::Result as StdResult;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use fuse3::notify::Notify;
use fuse3::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr, ReplyAttr, ReplyBmap, ReplyCopyFileRange,
    ReplyCreated, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyLSeek,
    ReplyOpen, ReplyPoll, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use fuse3::raw::{Filesystem, Request};
use fuse3::{FileType, Inode, Result, SetAttr, Timestamp};
use futures::stream::BoxStream;
use futures::StreamExt;
use futures_async_stream::stream;
use juice_meta::api::{Attr, Entry, Falloc, INodeType, OFlag, SetAttrMask};
use juice_utils::fs::{self, FsContext};
use juice_vfs::Vfs;
use nix::errno::Errno;
use tokio_util::sync::CancellationToken;

use crate::fuse_kernel;

const NANO_PER_SEC: u128 = 1_000_000_000;
pub struct JuiceFs {
    vfs: Vfs,
    ops: DashMap<u64, CancellationToken>,
}

impl JuiceFs {
    fn login(req: Request) -> FsContext {
        FsContext::new(req.uid, req.gid, req.pid, true)
    }

    async fn login_with_action<F>(&self, req: Request, f: F) -> F::Output
    where
        F: Future,
    {
        let ctx = Self::login(req);
        self.ops.insert(req.unique, ctx.token().clone());
        fs::task_local::scope(ctx, f).await
    }

    async fn reply_entry(vfs: &Vfs, entry: Entry) -> ReplyEntry {
        // TODO: distinguish attr ttl and entry ttl
        let duration = match entry.attr.typ {
            INodeType::Directory => vfs.config().dir_entry_timeout,
            _ => vfs.config().entry_timeout,
        }
        .map(|t| t.to_std().unwrap_or(Duration::ZERO))
        .unwrap_or(Duration::ZERO);
        let reply_attr = Self::reply_attr(vfs, entry).await;
        ReplyEntry {
            ttl: duration,
            attr: reply_attr.attr,
            generation: 1,
        }
    }

    async fn reply_attr(vfs: &Vfs, mut entry: Entry) -> ReplyAttr {
        // Notice: There needs to be a mechanism here to prevent the problem that thread A changes Attr in
        // multi-threading situations, and thread B is not visible to Attr at this time.
        let attr_ttl = if vfs.is_special_inode(entry.inode) {
            Duration::from_hours(1)
        } else if entry.attr.typ == INodeType::File && vfs.modified_since(&entry.inode) {
            if let Ok(updated_entry) = vfs.get_attr(entry.inode).await {
                entry.attr = updated_entry.attr;
                vfs.config()
                    .attr_timeout
                    .map(|t| t.to_std().unwrap_or(Duration::ZERO))
                    .unwrap_or(Duration::ZERO)
            } else {
                Duration::ZERO
            }
        } else {
            vfs.config()
                .attr_timeout
                .map(|t| t.to_std().unwrap_or(Duration::ZERO))
                .unwrap_or(Duration::ZERO)
        };
        vfs.update_len(entry.inode, &mut entry.attr);
        // convert
        let (size, block, rdev) = match entry.attr.typ {
            INodeType::Directory | INodeType::Symlink | INodeType::File => {
                (entry.attr.length, (entry.attr.length + 511) / 512, 0)
            }
            INodeType::BlockDev | INodeType::CharDev => (0, 0, entry.attr.rdev),
            _ => (0, 0, 0),
        };
        let atime = Timestamp::new(
            (entry.attr.atime / NANO_PER_SEC) as i64,
            (entry.attr.atime % NANO_PER_SEC) as u32,
        );
        let mtime = Timestamp::new(
            (entry.attr.mtime / NANO_PER_SEC) as i64,
            (entry.attr.mtime % NANO_PER_SEC) as u32,
        );
        let ctime = Timestamp::new(
            (entry.attr.ctime / NANO_PER_SEC) as i64,
            (entry.attr.ctime % NANO_PER_SEC) as u32,
        );
        let kind = match entry.attr.typ {
            INodeType::Directory => FileType::Directory,
            INodeType::File => FileType::RegularFile,
            INodeType::Symlink => FileType::Symlink,
            INodeType::FIFO => FileType::NamedPipe,
            INodeType::BlockDev => FileType::BlockDevice,
            INodeType::CharDev => FileType::CharDevice,
            INodeType::Socket => FileType::Socket,
        };
        ReplyAttr {
            ttl: attr_ttl,
            attr: FileAttr {
                ino: entry.inode,
                size: size,
                blocks: block,
                atime,
                mtime,
                ctime,
                kind,
                perm: entry.attr.mode,
                nlink: entry.attr.nlink,
                uid: entry.attr.uid,
                gid: entry.attr.gid,
                rdev: rdev,
                blksize: 0x10000,
            },
        }
    }

    fn into_file_type(mode: u32) -> Result<INodeType> {
        match mode & libc::S_IFMT {
            libc::S_IFIFO => Ok(INodeType::FIFO),
            libc::S_IFCHR => Ok(INodeType::CharDev),
            libc::S_IFBLK => Ok(INodeType::BlockDev),
            libc::S_IFDIR => Ok(INodeType::Directory),
            libc::S_IFREG => Ok(INodeType::File),
            libc::S_IFLNK => Ok(INodeType::Symlink),
            libc::S_IFSOCK => Ok(INodeType::Socket),
            _ => Err(libc::EINVAL.into()),
        }
    }

    fn from_file_type(typ: &INodeType) -> FileType {
        match typ {
            INodeType::Directory => FileType::Directory,
            INodeType::File => FileType::RegularFile,
            INodeType::Symlink => FileType::Symlink,
            INodeType::FIFO => FileType::NamedPipe,
            INodeType::BlockDev => FileType::BlockDevice,
            INodeType::CharDev => FileType::CharDevice,
            INodeType::Socket => FileType::Socket,
        }
    }

    #[stream(boxed, item = Result<DirectoryEntryPlus>)]
    async fn into_direntry_plus<'a>(
        &'a self,
        offset: u64,
        mut stream: BoxStream<'a, StdResult<Entry, Errno>>,
    ) {
        let mut next_off = offset;
        while let Some(result) = stream.next().await {
            match result {
                Ok(entry) => {
                    let kind = Self::from_file_type(&entry.attr.typ);
                    let name = OsString::from(entry.name.clone());
                    let inode = entry.inode;
                    // TODO: only full return attr?
                    let reply_entry = Self::reply_entry(&self.vfs, entry).await;
                    next_off += 1;
                    yield Ok(DirectoryEntryPlus {
                        inode,
                        kind,
                        name,
                        offset: next_off as i64,
                        generation: 1,
                        attr: reply_entry.attr,
                        entry_ttl: reply_entry.ttl,
                        attr_ttl: reply_entry.ttl,
                    });
                }
                Err(e) => yield Err((e as i32).into()),
            }
        }
    }
}

#[allow(unused_variables)]
impl Filesystem for JuiceFs {
    /// initialize filesystem. Called before any other filesystem method.
    async fn init(&self, req: Request) -> Result<ReplyInit> {
        todo!()
    }

    /// clean up filesystem. Called on filesystem exit which is fuseblk, in normal fuse filesystem,
    /// kernel may call forget for root. There is some discuss for this
    /// <https://github.com/bazil/fuse/issues/82#issuecomment-88126886>,
    /// <https://sourceforge.net/p/fuse/mailman/message/31995737/>
    async fn destroy(&self, req: Request) {}

    /// look up a directory entry by name and get its attributes.
    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> Result<ReplyEntry> {
        todo!()
    }

    /// forget an inode. The nlookup parameter indicates the number of lookups previously
    /// performed on this inode. If the filesystem implements inode lifetimes, it is recommended
    /// that inodes acquire a single reference on each lookup, and lose nlookup references on each
    /// forget. The filesystem may ignore forget calls, if the inodes don't need to have a limited
    /// lifetime. On unmount it is not guaranteed, that all referenced inodes will receive a forget
    /// message. When filesystem is normal(not fuseblk) and unmounting, kernel may send forget
    /// request for root and this library will stop session after call forget. There is some
    /// discussion for this <https://github.com/bazil/fuse/issues/82#issuecomment-88126886>,
    /// <https://sourceforge.net/p/fuse/mailman/message/31995737/>
    async fn forget(&self, req: Request, inode: Inode, nlookup: u64) {}

    /// get file attributes. If `fh` is None, means `fh` is not set.
    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> Result<ReplyAttr> {
        self.login_with_action(req, async {
            match self.vfs.get_attr(inode).await {
                Ok(entry) => Ok(Self::reply_attr(&self.vfs, entry).await),
                Err(e) => Err((e as i32).into()),
            }
        })
        .await
    }

    /// set file attributes. If `fh` is None, means `fh` is not set.
    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        self.login_with_action(req, async {
            let mut attr = Attr::default();
            let mut mask = SetAttrMask::empty();

            if let Some(mode) = set_attr.mode {
                mask |= SetAttrMask::SET_MODE;
                // mode only valid in 12bits
                attr.mode = (mode & 0o7777) as u16;
            }
            if let Some(uid) = set_attr.uid {
                mask |= SetAttrMask::SET_UID;
                attr.uid = uid;
            }
            if let Some(gid) = set_attr.gid {
                mask |= SetAttrMask::SET_GID;
                attr.gid = gid;
            }
            if let Some(size) = set_attr.size {
                mask |= SetAttrMask::SET_SIZE;
                attr.length = size;
            }
            if let Some(atime) = set_attr.atime {
                mask |= SetAttrMask::SET_ATIME;
                attr.atime = atime.sec as u128 * NANO_PER_SEC + atime.nsec as u128;
            }
            if let Some(mtime) = set_attr.mtime {
                mask |= SetAttrMask::SET_MTIME;
                attr.mtime = mtime.sec as u128 * NANO_PER_SEC + mtime.nsec as u128;
            }
            if let Some(ctime) = set_attr.ctime {
                mask |= SetAttrMask::SET_CTIME;
                attr.ctime = ctime.sec as u128 * NANO_PER_SEC + ctime.nsec as u128;
            }
            match self.vfs.set_attr(inode, mask, fh, attr).await {
                Ok(entry) => Ok(Self::reply_attr(&self.vfs, entry).await),
                Err(e) => Err((e as i32).into()),
            }
        })
        .await
    }

    /// read symbolic link.
    async fn readlink(&self, req: Request, inode: Inode) -> Result<ReplyData> {
        Err(libc::ENOSYS.into())
    }

    /// create a symbolic link.
    async fn symlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> Result<ReplyEntry> {
        Err(libc::ENOSYS.into())
    }

    /// create file node. Create a regular file, character device, block device, fifo or socket
    /// node. When creating file, most cases user only need to implement
    /// [`create`][Filesystem::create].
    async fn mknod(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<ReplyEntry> {
        self.login_with_action(req, async {
            let typ = Self::into_file_type(mode)?;
            // TODO: [`FileSystem`] mknod not support umasks
            match self
                .vfs
                .mknod(
                    parent,
                    &name.to_string_lossy(),
                    typ,
                    (mode & 0xffff) as u16,
                    0,
                    rdev,
                )
                .await
            {
                Ok(entry) => Ok(Self::reply_entry(&self.vfs, entry).await),
                Err(e) => Err((e as i32).into()),
            }
        })
        .await
    }

    /// create a directory.
    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> Result<ReplyEntry> {
        self.login_with_action(req, async {
            // TODO: [`FileSystem`] mknod not support umasks
            match self
                .vfs
                .mkdir(parent, &name.to_string_lossy(), (mode & 0xffff) as u16, 0)
                .await
            {
                Ok(entry) => Ok(Self::reply_entry(&self.vfs, entry).await),
                Err(e) => Err((e as i32).into()),
            }
        })
        .await
    }

    /// remove a file.
    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// remove a directory.
    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// rename a file or directory.
    async fn rename(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// create a hard link.
    async fn link(
        &self,
        req: Request,
        inode: Inode,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        Err(libc::ENOSYS.into())
    }

    /// open a file. Open flags (with the exception of `O_CREAT`, `O_EXCL` and `O_NOCTTY`) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index, etc) in
    /// fh, and use this in other all other file operations (read, write, flush, release, fsync).
    /// Filesystem may also implement stateless file I/O and not store anything in fh. There are
    /// also some flags (`direct_io`, `keep_cache`) which the filesystem may set, to change the way
    /// the file is opened. A filesystem need not implement this method if it
    /// sets [`MountOptions::no_open_support`][crate::MountOptions::no_open_support] and if the
    /// kernel supports `FUSE_NO_OPEN_SUPPORT`.
    ///
    /// # Notes:
    ///
    /// See `fuse_file_info` structure in
    /// [fuse_common.h](https://libfuse.github.io/doxygen/include_2fuse__common_8h_source.html) for
    /// more details.
    async fn open(&self, req: Request, inode: Inode, mut flags: u32) -> Result<ReplyOpen> {
        self.login_with_action(req, async {
            self.vfs
                .open(inode, OFlag::from_bits_truncate(flags))
                .await
                .map(|(fh, attr)| {
                    if self.vfs.is_special_inode(inode) {
                        flags |= fuse_kernel::FOPEN_DIRECT_IO;
                    } else if attr.keep_cache {
                        flags |= fuse_kernel::FOPEN_KEEP_CACHE;
                    }
                    ReplyOpen { fh, flags }
                })
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// read data. Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to this is
    /// when the file has been opened in `direct_io` mode, in which case the return value of the
    /// read system call will reflect the return value of this operation. `fh` will contain the
    /// value set by the open method, or will be undefined if the open method didn't set any value.
    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        self.login_with_action(req, async {
            self.vfs
                .read(inode, fh, offset, size as u64)
                .await
                .map(|res| res.into())
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// write data. Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in `direct_io` mode, in which case the
    /// return value of the write system call will reflect the return value of this operation. `fh`
    /// will contain the value set by the open method, or will be undefined if the open method
    /// didn't set any value. When `write_flags` contains
    /// [`FUSE_WRITE_CACHE`](crate::raw::flags::FUSE_WRITE_CACHE), means the write operation is a
    /// delay write.
    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> Result<ReplyWrite> {
        self.login_with_action(req, async {
            self.vfs
                .write(inode, fh, offset, Bytes::copy_from_slice(data))
                .await
                .map(|res| ReplyWrite {
                    written: data.len() as u32,
                })
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// get filesystem statistics.
    async fn statfs(&self, req: Request, inode: Inode) -> Result<ReplyStatFs> {
        self.login_with_action(req, async {
            self.vfs
                .stat_fs(inode)
                .await
                .map(|res| {
                    let namelen = 255;
                    let frsize = 4096;
                    let bsize = 4096_u32;
                    let blocks = max(res.space_total / bsize as u64, 1);
                    let bavail = res.space_avail / bsize as u64;
                    let bfree = bavail;
                    let files = res.i_used;
                    let ffree = res.i_avail;
                    ReplyStatFs {
                        namelen,
                        frsize,
                        bsize,
                        blocks,
                        bavail,
                        bfree,
                        files,
                        ffree,
                    }
                })
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// release an open file. Release is called when there are no more references to an open file:
    /// all file descriptors are closed and all memory mappings are unmapped. For every open call
    /// there will be exactly one release call. The filesystem may reply with an error, but error
    /// values are not returned to `close()` or `munmap()` which triggered the release. `fh` will
    /// contain the value set by the open method, or will be undefined if the open method didn't
    /// set any value. `flags` will contain the same flags as for open. `flush` means flush the
    /// data or not when closing file.
    async fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        self.login_with_action(req, async {
            self.vfs
                .release(inode, fh)
                .await
                .map(|r| r)
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// synchronize file contents. If the `datasync` is true, then only the user data should be
    /// flushed, not the metadata.
    async fn fsync(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        // TODO: support datasync
        self.login_with_action(req, async {
            self.vfs
                .fsync(inode, fh)
                .await
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// set an extended attribute.
    async fn setxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// Get an extended attribute. If `size` is too small, return `Err<ERANGE>`.
    /// Otherwise, use [`ReplyXAttr::Data`] to send the attribute data, or
    /// return an error.
    async fn getxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr> {
        Err(libc::ENOSYS.into())
    }

    /// List extended attribute names.
    ///
    /// If `size` is too small, return `Err<ERANGE>`.  Otherwise, use
    /// [`ReplyXAttr::Data`] to send the attribute list, or return an error.
    async fn listxattr(&self, req: Request, inode: Inode, size: u32) -> Result<ReplyXAttr> {
        Err(libc::ENOSYS.into())
    }

    /// remove an extended attribute.
    async fn removexattr(&self, req: Request, inode: Inode, name: &OsStr) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// flush method. This is called on each `close()` of the opened file. Since file descriptors
    /// can be duplicated (`dup`, `dup2`, `fork`), for one open call there may be many flush calls.
    /// Filesystems shouldn't assume that flush will always be called after some writes, or that if
    /// will be called at all. `fh` will contain the value set by the open method, or will be
    /// undefined if the open method didn't set any value.
    ///
    /// # Notes:
    ///
    /// the name of the method is misleading, since (unlike fsync) the filesystem is not forced to
    /// flush pending writes. One reason to flush data, is if the filesystem wants to return write
    /// errors. If the filesystem supports file locking operations ([`setlk`][Filesystem::setlk],
    /// [`getlk`][Filesystem::getlk]) it should remove all locks belonging to `lock_owner`.
    async fn flush(&self, req: Request, inode: Inode, fh: u64, lock_owner: u64) -> Result<()> {
        self.login_with_action(req, async {
            self.vfs
                .flush(inode, fh, lock_owner)
                .await
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// open a directory. Filesystem may store an arbitrary file handle (pointer, index, etc) in
    /// `fh`, and use this in other all other directory stream operations
    /// ([`readdir`][Filesystem::readdir], [`releasedir`][Filesystem::releasedir],
    /// [`fsyncdir`][Filesystem::fsyncdir]). Filesystem may also implement stateless directory
    /// I/O and not store anything in `fh`.  A file system need not implement this method if it
    /// sets [`MountOptions::no_open_dir_support`][crate::MountOptions::no_open_dir_support] and
    /// if the kernel supports `FUSE_NO_OPENDIR_SUPPORT`.
    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        self.login_with_action(req, async {
            self.vfs
                .opendir(inode, OFlag::from_bits_truncate(flags))
                .await
                .map(|fh| ReplyOpen {
                    fh,
                    flags: 0, // open dir doesn't support open flags
                })
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// dir entry stream given by [`readdir`][Filesystem::readdir].
    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;

    /// read directory. `offset` is used to track the offset of the directory entries. `fh` will
    /// contain the value set by the [`opendir`][Filesystem::opendir] method, or will be
    /// undefined if the [`opendir`][Filesystem::opendir] method didn't set any value.
    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let entries = self
            .login_with_action(req, async {
                let mut next_off = offset;
                Box::pin(
                    self.vfs
                        .readdir(parent, fh, offset as u64)
                        .map(move |result| {
                            result
                                .map(|entry| {
                                    next_off += 1;
                                    DirectoryEntry {
                                        inode: entry.inode,
                                        kind: Self::from_file_type(&entry.attr.typ),
                                        name: OsString::from(entry.name),
                                        offset: next_off,
                                    }
                                })
                                .map_err(|e| (e as i32).into())
                        }),
                )
            })
            .await;
        Ok(ReplyDirectory { entries })
    }

    /// release an open directory. For every [`opendir`][Filesystem::opendir] call there will
    /// be exactly one `releasedir` call. `fh` will contain the value set by the
    /// [`opendir`][Filesystem::opendir] method, or will be undefined if the
    /// [`opendir`][Filesystem::opendir] method didn't set any value.
    async fn releasedir(&self, req: Request, inode: Inode, fh: u64, flags: u32) -> Result<()> {
        self.login_with_action(req, async {
            self.vfs
                .releasedir(inode, fh)
                .await
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// synchronize directory contents. If the `datasync` is true, then only the directory contents
    /// should be flushed, not the metadata. `fh` will contain the value set by the
    /// [`opendir`][Filesystem::opendir] method, or will be undefined if the
    /// [`opendir`][Filesystem::opendir] method didn't set any value.
    async fn fsyncdir(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    #[cfg(feature = "file-lock")]
    /// test for a POSIX file lock.
    ///
    /// # Notes:
    ///
    /// this is supported on enable **`file-lock`** feature.
    #[allow(clippy::too_many_arguments)]
    async fn getlk(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
    ) -> Result<ReplyLock>;

    #[cfg(feature = "file-lock")]
    /// acquire, modify or release a POSIX file lock.
    ///
    /// # Notes:
    ///
    /// this is supported on enable **`file-lock`** feature.
    #[allow(clippy::too_many_arguments)]
    async fn setlk(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
        block: bool,
    ) -> Result<()>;

    /// check file access permissions. This will be called for the `access()` system call. If the
    /// `default_permissions` mount option is given, this method is not be called. This method is
    /// not called under Linux kernel versions 2.4.x.
    async fn access(&self, req: Request, inode: Inode, mask: u32) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// create and open a file. If the file does not exist, first create it with the specified
    /// mode, and then open it. Open flags (with the exception of `O_NOCTTY`) are available in
    /// flags. Filesystem may store an arbitrary file handle (pointer, index, etc) in `fh`, and use
    /// this in other all other file operations ([`read`][Filesystem::read],
    /// [`write`][Filesystem::write], [`flush`][Filesystem::flush],
    /// [`release`][Filesystem::release], [`fsync`][Filesystem::fsync]). There are also some flags
    /// (`direct_io`, `keep_cache`) which the filesystem may set, to change the way the file is
    /// opened. If this method is not implemented or under Linux kernel versions earlier than
    /// 2.6.15, the [`mknod`][Filesystem::mknod] and [`open`][Filesystem::open] methods will be
    /// called instead.
    ///
    /// # Notes:
    ///
    /// See `fuse_file_info` structure in
    /// [fuse_common.h](https://libfuse.github.io/doxygen/include_2fuse__common_8h_source.html) for
    /// more details.
    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        self.login_with_action(req, async {
            match self
                .vfs
                .create(
                    parent,
                    &name.to_string_lossy(),
                    (mode & 0xffff) as u16,
                    0,
                    OFlag::from_bits_truncate(flags),
                )
                .await
            {
                Ok((fh, entry)) => {
                    let reply_entry = Self::reply_entry(&self.vfs, entry).await;
                    Ok(ReplyCreated {
                        ttl: reply_entry.ttl,
                        attr: reply_entry.attr,
                        generation: reply_entry.generation,
                        fh,
                        flags: 0, // wait open to fill it
                    })
                }
                Err(e) => Err((e as i32).into()),
            }
        })
        .await
    }

    /// handle interrupt. When a operation is interrupted, an interrupt request will send to fuse
    /// server with the unique id of the operation.
    async fn interrupt(&self, req: Request, unique: u64) -> Result<()> {
        if let Some(token) = self.ops.get(&unique) {
            token.cancel();
        }
        Ok(())
    }

    /// map block index within file to block index within device.
    ///
    /// # Notes:
    ///
    /// This may not works because currently this crate doesn't support fuseblk mode yet.
    async fn bmap(
        &self,
        req: Request,
        inode: Inode,
        blocksize: u32,
        idx: u64,
    ) -> Result<ReplyBmap> {
        Err(libc::ENOSYS.into())
    }

    /*async fn ioctl(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        cmd: u32,
        arg: u64,
        in_size: u32,
        out_size: u32,
    ) -> Result<ReplyIoctl> {
        Err(libc::ENOSYS.into())
    }*/

    /// poll for IO readiness events.
    #[allow(clippy::too_many_arguments)]
    async fn poll(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        kh: Option<u64>,
        flags: u32,
        events: u32,
        notify: &Notify,
    ) -> Result<ReplyPoll> {
        Err(libc::ENOSYS.into())
    }

    /// receive notify reply from kernel.
    async fn notify_reply(
        &self,
        req: Request,
        inode: Inode,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// forget more than one inode. This is a batch version [`forget`][Filesystem::forget]
    async fn batch_forget(&self, req: Request, inodes: &[Inode]) {}

    /// allocate space for an open file. This function ensures that required space is allocated for
    /// specified file.
    ///
    /// # Notes:
    ///
    /// more information about `fallocate`, please see **`man 2 fallocate`**
    async fn fallocate(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<()> {
        self.login_with_action(req, async {
            self.vfs
                .fallocate(
                    inode,
                    fh,
                    Falloc::from_bits_truncate(mode as u8),
                    offset,
                    length,
                )
                .await
                .map_err(|e| (e as i32).into())
        })
        .await
    }

    /// dir entry plus stream given by [`readdirplus`][Filesystem::readdirplus].
    type DirEntryPlusStream<'a> = BoxStream<'a, Result<DirectoryEntryPlus>>;

    /// read directory entries, but with their attribute, like [`readdir`][Filesystem::readdir]
    /// + [`lookup`][Filesystem::lookup] at the same time.
    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let entries = self
            .login_with_action(req, async {
                self.into_direntry_plus(offset, self.vfs.readdir(parent, fh, offset as u64))
            })
            .await;
        Ok(ReplyDirectoryPlus { entries })
    }

    /// rename a file or directory with flags.
    async fn rename2(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: u32,
    ) -> Result<()> {
        Err(libc::ENOSYS.into())
    }

    /// find next data or hole after the specified offset.
    async fn lseek(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        Err(libc::ENOSYS.into())
    }

    /// copy a range of data from one file to another. This can improve performance because it
    /// reduce data copy: in normal, data will copy from FUSE server to kernel, then to user-space,
    /// then to kernel, finally send back to FUSE server. By implement this method, data will only
    /// copy in FUSE server internal.
    #[allow(clippy::too_many_arguments)]
    async fn copy_file_range(
        &self,
        req: Request,
        inode: Inode,
        fh_in: u64,
        off_in: u64,
        inode_out: Inode,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        self.login_with_action(req, async {
            self.vfs
                .copy_file_range(
                    inode,
                    fh_in,
                    off_in,
                    inode_out,
                    fh_out,
                    off_out,
                    length,
                    flags as u32,
                )
                .await
                .map(|copied| ReplyCopyFileRange { copied })
                .map_err(|e| (e as i32).into())
        })
        .await
    }
}
