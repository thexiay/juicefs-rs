use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use tracing::{debug, error, info, warn};

use crate::api::{INodeType, Ino, Meta, Summary, ROOT_INODE};
use crate::base::{CommonMeta, DirStat, Engine};
use crate::error::{NoSpaceSnafu, QuotaExceededSnafu, Result};
use crate::utils::align_4k;

#[derive(Default, Debug)]
pub struct Quota {
    pub max_space: AtomicI64,
    pub max_inodes: AtomicI64,
    pub used_space: AtomicI64,
    pub used_inodes: AtomicI64,
    pub new_space: AtomicI64,
    pub new_inodes: AtomicI64,
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
        self.new_space.fetch_add(space, std::sync::atomic::Ordering::SeqCst);
        self.new_inodes.fetch_add(inodes, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
pub trait MetaQuota {
    async fn get_dir_parent(&self, inode: Ino) -> Result<Ino>;
    fn update_dir_stats(&self, inode: Ino, length: i64, space: i64, inodes: i64);
    async fn update_parent_stats(&self, inode: Ino, parent: Ino, length: i64, space: i64);

    /// check if the space and inodes exceed the quota limit for parents ino
    async fn check_quotas(&self, space: i64, inodes: i64, parents: Vec<Ino>) -> Result<()>;

    /// check if the space and inodes exceed the quota limit for single ino
    async fn check_quota(&self, ino: Ino, space: i64, inodes: i64) -> bool;

    /// update the new space and new inode for quota
    async fn update_quota(&self, inode: Ino, space: i64, inodes: i64);

    /// load quotas from persist layer
    async fn load_quotas(&self);

    /// flush quotas once
    async fn flush_quotas(&self);

    /// flush dir stats once
    async fn flush_dir_stat(&self);

    /// get inode of the first parent (or myself) with quota
    async fn get_quota_parent(&self, ino: Ino) -> Option<Ino>;

    /// Get summary of a dir ino
    async fn get_dir_summary(&self, ino: Ino, recursive: bool, strict: bool) -> Result<Summary>;
    
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
                let mut quotas = base.dir_quotas.write();

                quotas.iter().for_each(|(ino, _)| {
                    if !loaded_quotas.contains_key(ino) {
                        // quota cache should be consistent with the meta persist layer
                        error!("Quota for inode {} is deleted", ino);
                    }
                });

                loaded_quotas.iter().for_each(|(ino, quota)| {
                    if let Some(q) = quotas.get(ino) {
                        quota
                            .new_space
                            .fetch_add(q.new_space.load(Ordering::SeqCst), Ordering::SeqCst);
                        quota
                            .new_inodes
                            .fetch_add(q.new_inodes.load(Ordering::SeqCst), Ordering::SeqCst);
                    }
                });
                *quotas = loaded_quotas;
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
            Err(e) => warn!("Flush quotas: {}", e),
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
            self.do_flush_dir_stat(dir_stats).await;
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
}