#![feature(assert_matches)]
mod admin;
mod parser;
mod service;
use std::path::PathBuf;

use admin::{juice_format, AdminCommands};
use clap::Parser;
use juice_utils::runtime::{init_juicefs_logger, main_okk, LogTarget, LoggerSettings};
use service::{juice_mount, ServiceCommands};
use snafu::Whatever;
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
                let settings =  {
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
        CliOpts::Tool => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use clap::Parser;

    use crate::{admin::AdminCommands, service::ServiceCommands, CliOpts};

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
        assert_matches!(
            opts,
            CliOpts::Admin(AdminCommands::Format(_))
        );
    }
}