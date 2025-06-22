use std::{fs, process};

use nix::sys::stat::{Mode, SFlag};
use snafu::{whatever, ResultExt, Whatever};

use crate::JuiceFs;

impl JuiceFs {
    pub fn set_priority() -> Result<(), Whatever> {
        let ret =
            unsafe { libc::setpriority(libc::PRIO_PROCESS, process::id(), libc::PRIO_MIN + 1) };
        if ret != 0 {
            whatever!("Syscall setpriority: {}", ret);
        }
        Ok(())
    }

    /// You may not have authorized permission to access fuse. You need to use the tricy method to find the device 
    /// permission control in the cgroup and grant it access to the fuse device
    pub fn grant_fuse_access() -> Result<(), Whatever> {
        // TODO: find cgroup devices.allow and add access to 'c 10:229 rwm'
        // (https://www.kernel.org/doc/Documentation/admin-guide/devices.txt)
        Ok(())
    }

    pub fn ensure_fuse_dev() -> Result<(), Whatever> {
        if !std::fs::exists("/dev/fuse").whatever_context("exists /dev/fuse")? {
            nix::sys::stat::mknod(
                "/dev/fuse",
                nix::sys::stat::SFlag::S_IFCHR,
                Mode::from_bits_retain(0o666),
                nix::sys::stat::makedev(10, 229),
            ).whatever_context("mknod /dev/fuse")?;
        }
        Ok(())
    }
}
