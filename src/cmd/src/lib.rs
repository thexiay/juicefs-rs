mod admin;
mod parser;
mod service;
use std::path::PathBuf;

use admin::{juice_format, AdminCommands};
use clap::Parser;
use juice_utils::runtime::{init_juicefs_logger, main_okk, LoggerSettings};
use service::{juice_mount, ServiceCommands};
use snafu::Whatever;
use tracing::error;

type Result<T, E = Whatever> = std::result::Result<T, E>;

#[derive(Parser)]
#[command(version,
    about = "The DevOps tool that provides internal access to the RisingWave cluster",
    long_about = None,
    propagate_version = true,
    infer_subcommands = true
)]
pub enum CliOpts {
    #[command(flatten)]
    Admin(AdminCommands),
    Inspector,
    #[command(flatten)]
    Service(ServiceCommands),
    Tool,
}

pub fn cmd(opts: CliOpts) {
    match opts {
        CliOpts::Admin(admin_commands) => match admin_commands {
            AdminCommands::Format(format_opts) => {
                init_juicefs_logger(LoggerSettings::new("format"));
                main_okk(|_| {
                    Box::pin(async move {
                        if let Err(e) = juice_format(format_opts).await {
                            error!("Juice format failed: {e}");
                        }
                    })
                });
            }
            _ => (),
        },
        CliOpts::Inspector => todo!(),
        CliOpts::Service(service_commands) => match service_commands {
            ServiceCommands::Mount(mount_opts) => {
                let mut settings = LoggerSettings::new("mount");
                settings.with_log(PathBuf::from(&mount_opts.log_path));
                init_juicefs_logger(settings);
                main_okk(|shutdown| {
                    Box::pin(async move {
                        juice_mount(shutdown, mount_opts).await;
                    })
                });
            }
        },
        CliOpts::Tool => todo!(),
    }
}

// -------------------------- mount -----------------------------------
fn prepare_mount_point() {
    /*
    var fi os.FileInfo
        var ino uint64
        err := utils.WithTimeout(func() error {
            var err error
            fi, err = os.Stat(mp)
            return err
        }, time.Second*3)
        if !strings.Contains(mp, ":") && err != nil {
            err2 := utils.WithTimeout(func() error {
                return os.MkdirAll(mp, 0777)
            }, time.Second*3)
            if err2 != nil {
                if os.IsExist(err2) || strings.Contains(err2.Error(), "timeout after 3s") {
                    // a broken mount point, umount it
                    logger.Infof("mountpoint %s is broken: %s, umount it", mp, err)
                    _ = doUmount(mp, true)
                } else {
                    logger.Fatalf("create %s: %s", mp, err2)
                }
            }
        } else if err == nil {
            ino, _ = utils.GetFileInode(mp)
            if ino <= uint64(meta.RootInode) && fi.Size() == 0 {
                // a broken mount point, umount it
                logger.Infof("mountpoint %s is broken (ino=%d, size=%d), umount it", mp, ino, fi.Size())
                _ = doUmount(mp, true)
            }
        }

        if os.Getuid() == 0 {
            return
        }
        if ino == uint64(meta.RootInode) {
            return
        }
        switch runtime.GOOS {
        case "darwin":
            if fi, err := os.Stat(mp); err == nil {
                if st, ok := fi.Sys().(*syscall.Stat_t); ok {
                    if st.Uid != uint32(os.Getuid()) {
                        logger.Fatalf("current user should own %s", mp)
                    }
                }
            }
        case "linux":
            f, err := os.CreateTemp(mp, ".test")
            if err != nil && (os.IsPermission(err) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EROFS)) {
                logger.Fatalf("Do not have write permission on %s", mp)
            } else if f != nil {
                _ = f.Close()
                _ = os.Remove(f.Name())
            }
        }
     */
}
