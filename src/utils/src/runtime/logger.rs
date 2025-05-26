use std::path::PathBuf;
use std::str::FromStr;

use either::Either;
use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, filter};

use crate::env_var::env_var_is_true;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogTarget {
    /// The target name for the log.
    pub target: String,
    /// The level filter for the target.
    pub level: tracing::metadata::LevelFilter,
}

impl FromStr for LogTarget {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, '=');
        let target = parts
            .next()
            .ok_or("missing target name")?
            .trim()
            .to_string();
        let level = parts
            .next()
            .ok_or("missing target level")?
            .trim()
            .parse::<tracing::metadata::LevelFilter>()
            .map_err(|_| format!("invalid level filter for target `{}`", target))?;
        Ok(LogTarget { target, level })
    }
}

pub struct LoggerSettings {
    /// The name of the service. Used to identify the service in distributed tracing.
    name: String,
    /// Enable tokio console output.
    enable_tokio_console: bool,
    /// Enable colorful output in console.
    colorful: bool,
    /// Output to LOG_PATH instead of `stdout`.
    log: Option<PathBuf>,
    /// Whether to include thread name in the log.
    with_thread_name: bool,
    /// Override target settings.
    targets: Vec<(String, tracing::metadata::LevelFilter)>,
    /// Override the default level.
    default_level: Option<tracing::metadata::LevelFilter>,
}

impl Default for LoggerSettings {
    fn default() -> Self {
        Self::new("risingwave")
    }
}

impl LoggerSettings {
    /// Create a new logger settings with the given service name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enable_tokio_console: false,
            colorful: console::colors_enabled_stderr() && console::colors_enabled(),
            log: None,
            with_thread_name: false,
            targets: vec![],
            default_level: None,
        }
    }

    /// Enable tokio console output.
    pub fn tokio_console(mut self, enabled: bool) -> Self {
        self.enable_tokio_console = enabled;
        self
    }

    /// Enable write into stdout.
    pub fn with_log(mut self, log: PathBuf) -> Self {
        self.log = Some(log);
        self
    }

    /// Whether to include thread name in the log.
    pub fn with_thread_name(mut self, enabled: bool) -> Self {
        self.with_thread_name = enabled;
        self
    }

    /// Overrides the default target settings.
    pub fn with_target(
        mut self,
        target: impl Into<String>,
        level: impl Into<tracing::metadata::LevelFilter>,
    ) -> Self {
        self.targets.push((target.into(), level.into()));
        self
    }

    /// Overrides the default level.
    pub fn with_default(mut self, level: impl Into<tracing::metadata::LevelFilter>) -> Self {
        self.default_level = Some(level.into());
        self
    }
}

pub fn init_juicefs_logger(settings: LoggerSettings) {
    // Default timer for logging with local time offset.
    let default_timer = OffsetTime::local_rfc_3339().unwrap_or_else(|e| {
        println!(
            "failed to get local time offset, falling back to UTC: {}",
            e
        );
        OffsetTime::new(
            time::UtcOffset::UTC,
            time::format_description::well_known::Rfc3339,
        )
    });

    // Default filter for logging to stdout and tracing.
    let default_filter = {
        let mut filter = filter::Targets::new();
        let default_level = if cfg!(debug_assertions) {
            Level::DEBUG
        } else {
            Level::INFO
        };
        filter = filter.with_default(default_level);

        // Overrides from settings.
        filter = filter.with_targets(settings.targets);
        if let Some(default_level) = settings.default_level {
            filter = filter.with_default(default_level);
        }

        // Overrides from env var.
        if let Ok(rust_log) = std::env::var(EnvFilter::DEFAULT_ENV)
            && !rust_log.is_empty()
        {
            let rust_log_targets: Targets = rust_log.parse().expect("failed to parse `RUST_LOG`");
            if let Some(default_level) = rust_log_targets.default_level() {
                filter = filter.with_default(default_level);
            }
            filter = filter.with_targets(rust_log_targets)
        };

        filter
    };

    let mut layers = vec![];

    // fmt layer (formatting and logging to `juicefs.log`)
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_names(settings.with_thread_name)
            .with_timer(default_timer.clone())
            .with_ansi(settings.colorful)
            .with_writer(move || {
                if let Some(ref log) = settings.log {
                    std::fs::create_dir_all(log.clone()).unwrap_or_else(|e| {
                        panic!(
                            "failed to create directory '{:?}' for query log: {}",
                            log, e,
                        )
                    });

                    let path = log.join("juicefs-rs.log");
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .append(true)
                        .open(&path)
                        .unwrap_or_else(
                            |e| panic!("failed to create `{}`: {}", path.display(), e,),
                        );
                    Either::Right(file)
                } else {
                    Either::Left(std::io::stdout())
                }
            });

        let fmt_layer = if env_var_is_true("ENABLE_PRETTY_LOG") {
            fmt_layer.pretty().boxed()
        } else {
            fmt_layer.boxed()
        };

        layers.push(fmt_layer.with_filter(default_filter).boxed());
    };

    if settings.enable_tokio_console {
        let (console_layer, server) = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .build();
        let console_layer = console_layer.with_filter(
            filter::Targets::new()
                .with_target("tokio", Level::TRACE)
                .with_target("runtime", Level::TRACE),
        );
        layers.push(console_layer.boxed());
        std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    println!("serving console subscriber");
                    server.serve().await.unwrap();
                });
        });
    };

    // Metrics layer
    tracing_subscriber::registry().with(layers).init();
}
