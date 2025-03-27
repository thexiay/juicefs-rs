use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::{debug, error, warn};

use crate::api::{INodeType, Ino, Meta, StatFs, Summary, ROOT_INODE};
use crate::base::{CommonMeta, DirStat, Engine, USED_INODES, USED_SPACE};
use crate::error::{NoSpaceSnafu, QuotaExceededSnafu, Result};
use crate::utils::align_4k;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaView {
    pub max_space: u64,
    pub max_inodes: u64,
    pub used_space: u64,
    pub used_inodes: u64,
}

impl QuotaView {
    // Format the maximum data capacity to match the data capacity that has been used
    pub fn sanitize(&mut self) {
        if self.max_space > 0 && self.max_space < self.used_space {
            self.max_space = self.used_space;
        }
        if self.max_inodes > 0 && self.max_inodes < self.used_inodes {
            self.max_inodes = self.used_inodes;
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct Quota {
    pub max_space: AtomicI64,
    pub max_inodes: AtomicI64,
    pub used_space: AtomicI64,
    pub used_inodes: AtomicI64,
    pub new_space: AtomicI64,
    pub new_inodes: AtomicI64,
}

impl From<QuotaView> for Quota {
    fn from(value: QuotaView) -> Self {
        Quota {
            max_space: AtomicI64::new(value.max_space as i64),
            max_inodes: AtomicI64::new(value.max_inodes as i64),
            used_space: AtomicI64::new(value.used_space as i64),
            used_inodes: AtomicI64::new(value.used_inodes as i64),
            new_space: AtomicI64::new(0),
            new_inodes: AtomicI64::new(0),
        }
    }
}

impl Quota {
    // Returns true if it will exceed the quota limit
    pub fn check(&self, space: i64, inodes: i64) -> bool {
        if space > 0 {
            let max = self.max_space.load(std::sync::atomic::Ordering::SeqCst);
            if max > 0
                && self.used_space.load(std::sync::atomic::Ordering::SeqCst)
                    + self.new_space.load(std::sync::atomic::Ordering::SeqCst)
                    + space
                    > max
            {
                return true;
            }
        }
        if inodes > 0 {
            let max = self.max_inodes.load(std::sync::atomic::Ordering::SeqCst);
            if max > 0
                && self.used_inodes.load(std::sync::atomic::Ordering::SeqCst)
                    + self.new_inodes.load(std::sync::atomic::Ordering::SeqCst)
                    + inodes
                    > max
            {
                return true;
            }
        }
        return false;
    }

    pub fn update(&self, space: i64, inodes: i64) {
        self.new_space
            .fetch_add(space, std::sync::atomic::Ordering::SeqCst);
        self.new_inodes
            .fetch_add(inodes, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
pub(crate) trait MetaQuota {
    async fn get_dir_parent(&self, inode: Ino) -> Result<Ino>;
    fn update_dir_stats(&self, inode: Ino, length: i64, space: i64, inodes: i64);
    async fn update_parent_stats(&self, inode: Ino, parent: Ino, length: i64, space: i64);
    async fn update_stats(&self, space: i64, inodes: i64);
    async fn calc_dir_stat(&self, ino: Ino) -> Result<DirStat>;

    /// check if the space and inodes exceed the quota limit for parents ino
    async fn check_quotas(&self, space: i64, inodes: i64, parents: Vec<Ino>) -> Result<()>;

    /// check if the space and inodes exceed the quota limit for single ino
    async fn check_quota(&self, ino: Ino, space: i64, inodes: i64) -> bool;

    /// update the new space and new inode for quota
    async fn update_quota(&self, inode: Ino, space: i64, inodes: i64);

    /// Load all quotas from persist layer and cache it.
    async fn load_quotas(&self);

    /// Flush all quotas cache into persist layer.
    async fn flush_quotas(&self);

    /// Flush dir stats into persist layer.
    async fn flush_dir_stat(&self);

    /// get inode of the first parent (or myself) with quota
    async fn get_quota_parent(&self, ino: Ino) -> Option<Ino>;

    /// Get summary of a dir ino
    async fn get_dir_summary(&self, ino: Ino, recursive: bool, strict: bool) -> Result<Summary>;

    /// Get the stat of the rootfs
    async fn stat_root_fs(&self) -> StatFs;
}

#[async_trait]
impl<E> MetaQuota for E
where
    E: Engine + AsRef<CommonMeta>,
{
    async fn get_dir_parent(&self, inode: Ino) -> Result<Ino> {
        let parent = {
            let guard = self.as_ref().dir_parents.lock();
            guard.get(&inode).map(|p| *p)
        };

        if let Some(parent) = parent {
            return Ok(parent);
        }
        debug!("Get directory parent of inode {}: cache miss", inode);
        let st = self.get_attr(inode).await?;
        Ok(st.parent)
    }

    fn update_dir_stats(&self, inode: Ino, length: i64, space: i64, inodes: i64) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        let mut dir_stats = self.as_ref().dir_stats_batch.lock();
        let stats = dir_stats.entry(inode).or_insert(DirStat::default());
        stats.length += length;
        stats.space += space;
        stats.inodes += inodes;
    }

    async fn update_parent_stats(&self, inode: Ino, parent: Ino, length: i64, space: i64) {
        if length == 0 && space == 0 {
            return;
        }
        self.update_stats(space, 0).await;
        if !self.get_format().enable_dir_stats {
            return;
        }

        if parent > 0 {
            self.update_dir_stats(parent, length, space, 0);
            self.update_quota(parent, space, 0).await;
        } else {
            // spawn a task to shorten the time of updating parent stats
            let meta = dyn_clone::clone_box(self);
            tokio::spawn(async move {
                for (parent, _) in meta.get_parents(inode).await {
                    meta.update_dir_stats(parent, length, space, 0);
                    meta.update_quota(parent, space, 0).await;
                }
            });
        }
    }

    async fn update_stats(&self, space: i64, inodes: i64) {
        self.as_ref().fs_stat.update_used_stats(space, inodes);
    }

    async fn calc_dir_stat(&self, ino: Ino) -> Result<DirStat> {
        let entries = self.do_readdir(ino, true, None).await?;
        let mut stat = DirStat::default();
        for entry in entries {
            stat.inodes += 1;
            if entry.attr.typ == INodeType::File {
                stat.length += entry.attr.length as i64;
                stat.space += align_4k(entry.attr.length);
            } else {
                stat.space += align_4k(0);
            }
        }
        Ok(stat)
    }

    async fn check_quotas(&self, space: i64, inodes: i64, parents: Vec<Ino>) -> Result<()> {
        if space <= 0 && inodes <= 0 {
            return Ok(());
        }
        let format = self.get_format();
        let meta = self.as_ref();
        if space > 0
            && format.capacity > 0
            && meta.fs_stat.used_space.load(Ordering::SeqCst)
                + meta.fs_stat.new_space.load(Ordering::SeqCst)
                + space
                > (format.capacity as i64)
        {
            return NoSpaceSnafu.fail()?;
        }
        if inodes > 0
            && format.inodes > 0
            && meta.fs_stat.used_inodes.load(Ordering::SeqCst)
                + meta.fs_stat.new_inodes.load(Ordering::SeqCst)
                + inodes
                > (format.capacity as i64)
        {
            return NoSpaceSnafu.fail()?;
        }
        if !format.enable_dir_stats {
            return Ok(());
        }
        for ino in parents {
            if self.check_quota(ino, space, inodes).await {
                return QuotaExceededSnafu.fail()?;
            }
        }
        Ok(())
    }

    async fn check_quota(&self, ino: Ino, space: i64, inodes: i64) -> bool {
        if !self.get_format().enable_dir_stats {
            return false;
        }

        let mut inode = ino;
        loop {
            {
                let guard = self.as_ref().dir_quotas.read();
                if let Some(quota) = guard.get(&inode)
                    && quota.check(space, inodes)
                {
                    return true;
                }
            }
            if inode <= ROOT_INODE {
                break;
            }
            let last_ino = inode;
            match self.get_dir_parent(inode).await {
                Ok(i) => inode = i,
                Err(e) => {
                    warn!("Get directory parent of inode {}: {}", last_ino, e);
                    break;
                }
            }
        }
        false
    }

    async fn update_quota(&self, mut inode: Ino, space: i64, inodes: i64) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        // recursively update the quota of the parent node until the root node is encountered
        loop {
            {
                let dir_quota = self.as_ref().dir_quotas.read();
                if let Some(quota) = dir_quota.get(&inode) {
                    quota.update(space, inodes);
                }
            }
            if inode <= ROOT_INODE {
                break;
            }
            inode = match self.get_dir_parent(inode).await {
                Ok(parent) => parent,
                Err(err) => {
                    warn!("Get directory parent of inode {} err: {}", inode, err);
                    break;
                }
            };
        }
    }

    async fn load_quotas(&self) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        // Here can not replcae directly, quotas's quota maybe concurrently accessed by other thread,
        // so we must modify it in place
        match self.do_load_quotas().await {
            Ok(loaded_quotas) => {
                let base = self.as_ref();
                let mut dir_quotas = base.dir_quotas.write();

                dir_quotas.retain(|ino, _| {
                    let retained = loaded_quotas.contains_key(ino);
                    if !loaded_quotas.contains_key(ino) {
                        // quota cache should be consistent with the meta persist layer
                        error!("Quota for inode {} is deleted", ino);
                    }
                    retained
                });

                loaded_quotas.into_iter().for_each(|(ino, quota)| {
                    dir_quotas
                        .entry(ino)
                        .and_modify(|dir_quota| {
                            dir_quota.max_space.store(quota.max_space as i64, Ordering::SeqCst);
                            dir_quota
                                .max_inodes
                                .store(quota.max_inodes as i64, Ordering::SeqCst);
                            dir_quota
                                .used_space
                                .store(quota.used_space as i64, Ordering::SeqCst);
                            dir_quota
                                .used_inodes
                                .store(quota.used_inodes as i64, Ordering::SeqCst);
                        })
                        .or_insert(Quota::from(quota));
                });
            }
            Err(e) => warn!("Load quotas: {}", e),
        }
    }

    async fn flush_quotas(&self) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        let base = self.as_ref();
        let stage_map = {
            let quotas = base.dir_quotas.read();
            let mut stage_map = HashMap::new();
            for (ino, quota) in quotas.iter() {
                let new_space = quota.new_space.load(Ordering::SeqCst);
                let new_inodes = quota.new_inodes.load(Ordering::SeqCst);
                if new_space != 0 || new_inodes != 0 {
                    stage_map.insert(*ino, (new_space, new_inodes));
                }
            }
            stage_map
        };
        if stage_map.is_empty() {
            return;
        }

        match self.do_flush_quotas(&stage_map).await {
            Ok(_) => {
                let mut quotas = base.dir_quotas.write();
                for (ino, snap) in stage_map {
                    let q = quotas.get_mut(&ino);
                    if let Some(q) = q {
                        q.new_space.fetch_sub(snap.0, Ordering::SeqCst);
                        q.used_space.fetch_add(snap.0, Ordering::SeqCst);
                        q.new_inodes.fetch_sub(snap.1, Ordering::SeqCst);
                        q.used_inodes.fetch_add(snap.1, Ordering::SeqCst);
                    }
                }
            }
            Err(e) => warn!("Flush quotas failed: {}", e),
        }
    }

    async fn flush_dir_stat(&self) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        let base = self.as_ref();
        let dir_stats = {
            let mut dir_stats = base.dir_stats_batch.lock();
            std::mem::take(&mut *dir_stats)
        };
        if !dir_stats.is_empty() {
            match self.do_flush_dir_stat(dir_stats).await {
                Ok(_) => {}
                Err(e) => error!("Flush dir stats failed: {}", e),
            }
        }
    }

    async fn get_quota_parent(&self, mut ino: Ino) -> Option<Ino> {
        if !self.get_format().enable_dir_stats {
            return None;
        }
        let base = self.as_ref();
        loop {
            {
                let dir_quotas = base.dir_quotas.read();
                if dir_quotas.contains_key(&ino) {
                    return Some(ino);
                }
            }
            if ino <= ROOT_INODE {
                break;
            }
            let last_ino = ino;
            ino = match self.get_dir_parent(ino).await {
                Ok(ino) => ino,
                Err(e) => {
                    warn!("Get directory parent of inode {}: {}", last_ino, e);
                    break;
                }
            }
        }
        None
    }

    async fn get_dir_summary(&self, ino: Ino, recursive: bool, strict: bool) -> Result<Summary> {
        // TODO: do it parallel
        // find all entry first, then collect summary
        let base = self.as_ref();
        let format = base.get_format();
        let mut summary = Summary::default();
        let entries = if strict || !format.enable_dir_stats {
            self.do_readdir(ino, true, None).await?
        } else {
            let dir_stat = self.get_dir_stat(ino).await?;
            let attr = self.do_get_attr(ino).await?;
            summary.size += dir_stat.space as u64;
            summary.length += dir_stat.length as u64;
            // TODO: here can not feagure why > 2 readdir
            if attr.nlink > 2 {
                self.do_readdir(ino, false, None).await?
            } else {
                summary.files += dir_stat.inodes as u64;
                Vec::new()
            }
        };

        for entry in entries {
            if entry.attr.typ == INodeType::Directory {
                summary.dirs += 1;
            } else {
                summary.files += 1;
            }
            if strict || !format.enable_dir_stats {
                summary.size += align_4k(entry.attr.length) as u64;
                if entry.attr.typ == INodeType::File {
                    summary.length += entry.attr.length as u64;
                }
            }
            if entry.attr.typ != INodeType::Directory || !recursive {
                continue;
            }
            match self.get_dir_summary(entry.inode, recursive, strict).await {
                Ok(sum) => summary += sum,
                Err(err) => {
                    if err.is_no_entry_found2(&entry.inode) {
                        warn!("inode({}) not found at collect summary", entry.inode);
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        Ok(summary)
    }

    async fn stat_root_fs(&self) -> StatFs {
        let meta = dyn_clone::clone_box(self);
        let err = timeout(Duration::from_millis(150), async {
            meta.get_counter(USED_SPACE).await
        })
        .await;
        let mut used_space = match err {
            Ok(Ok(space)) => space,
            other => {
                warn!("Get used space failed: {:?}. Use cache instead.", other);
                self.as_ref().fs_stat.used_space.load(Ordering::SeqCst)
            }
        };

        let meta = dyn_clone::clone_box(self);
        let err = timeout(Duration::from_millis(150), async {
            meta.get_counter(USED_INODES).await
        })
        .await;
        let mut used_inodes: i64 = match err {
            Ok(Ok(inodes)) => inodes,
            other => {
                warn!("Get used inodes failed: {:?}. Use cache instead.", other);
                self.as_ref().fs_stat.used_inodes.load(Ordering::SeqCst)
            }
        };

        used_space += self.as_ref().fs_stat.new_space.load(Ordering::SeqCst);
        used_inodes += self.as_ref().fs_stat.new_inodes.load(Ordering::SeqCst);
        if used_space < 0 {
            used_space = 0;
        }
        if used_inodes < 0 {
            used_inodes = 0;
        }

        // TODO: why format default value not set, below wired calculation
        let format = self.get_format();
        let total_space = if format.capacity > 0 {
            max(format.capacity, used_space as u64)
        } else {
            let mut total_space = 1 << 50;
            while total_space * 8 < used_space as u64 * 10 {
                total_space *= 2;
            }
            total_space
        };
        let availspace = total_space - used_space as u64;

        let iused = used_inodes as u64;
        let iavail = if format.inodes > 0 {
            if iused > format.inodes {
                0
            } else {
                format.inodes - iused
            }
        } else {
            let mut iavail = 10 << 20;
            while iused * 10 > (iused + iavail) * 8 {
                iavail *= 2;
            }
            iavail
        };
        StatFs {
            space_total: total_space,
            space_avail: availspace,
            i_used: iused,
            i_avail: iavail,
        }
    }
}
