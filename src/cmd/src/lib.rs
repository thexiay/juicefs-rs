#![feature(assert_matches)]
#![feature(async_closure)]
#![feature(unboxed_closures)]
#![feature(async_fn_traits)]
#![feature(let_chains)]
mod admin;
mod service;
mod tool;
use std::path::PathBuf;

use admin::{AdminCommands, juice_format};
use clap::Parser;
use dirs::home_dir;
use juice_utils::runtime::{LogTarget, LoggerSettings, init_juicefs_logger, main_okk};
use nix::unistd::getuid;
use service::{ServiceCommands, juice_mount};
use snafu::Whatever;
use tool::{juice_obj_bench, ToolCommands};
use tracing::error;

type Result<T, E = Whatever> = std::result::Result<T, E>;

#[derive(Parser, Debug)]
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
    #[command(flatten)]
    Tool(ToolCommands),
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
                let settings = {
                    let mut settings = LoggerSettings::new("mount")
                        .with_log(PathBuf::from(&mount_opts.log_path))
                        .with_thread_name(true);
                    for target in mount_opts.log_opts.log_targets.iter() {
                        settings = settings.with_target(&target.target, target.level);
                    }
                    settings
                };
                init_juicefs_logger(settings);
                main_okk(|shutdown| {
                    Box::pin(async move {
                        if let Err(e) = juice_mount(shutdown, &mount_opts).await {
                            error!("Juice mount failed: {e}");
                        }
                    })
                });
            }
        },
        CliOpts::Tool(tool_commands) => match tool_commands {
            ToolCommands::Bench(bench_opts) => {
                init_juicefs_logger(LoggerSettings::new("bench"));
            }
            ToolCommands::ObjBench(obj_bench_opts) => {
                init_juicefs_logger(LoggerSettings::new("obj-bench"));
                main_okk(|_| {
                    Box::pin(async move {
                        if let Err(e) = juice_obj_bench(obj_bench_opts).await {
                            error!("ObjBench failed: {e}");
                        }
                    })
                });
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use clap::Parser;

    use crate::{CliOpts, admin::AdminCommands, service::ServiceCommands};

    #[test]
    fn test_mount() {
        let args = [
            "juice",
            "test-juicefs-rs",
            "--storage",
            "cos",
            "--bucket",
            "test-bucket",
            "--endpoint",
            "https://cos.ap-guangzhou.myqcloud.com",
            "--access-key",
            "xxxxxxxxxxxxxx",
            "--secret-key",
            "xxxxxxxxxxxxxx",
            "--enable-acl",
            "format",
            "redis://:mypassword@127.0.1:11000",
        ];
        let opts = CliOpts::try_parse_from(args).unwrap();
        assert_matches!(opts, CliOpts::Admin(AdminCommands::Format(_)));
    }
}
