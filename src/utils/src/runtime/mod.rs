use std::pin::pin;
use std::process::ExitCode;

use futures::Future;

mod logger;
pub use logger::*;
mod tokio_rt;
use tokio::signal::unix::SignalKind;
pub use tokio_rt::*;
use tokio_util::sync::CancellationToken;
mod deadlock;
pub use deadlock::*;
mod panic_hook;
pub use panic_hook::*;
mod prof;
pub use prof::*;


const MIN_WORKER_THREADS: usize = 4;

/// Start Juice service components with configs from environment variable.
///
/// # Shutdown on Ctrl-C
///
/// The given closure `f` will take a [`CancellationToken`] as an argument. When a `SIGINT` signal
/// is received (typically by pressing `Ctrl-C`), [`CancellationToken::cancel`] will be called to
/// notify all subscribers to shutdown. You can use [`.cancelled()`](CancellationToken::cancelled)
/// to get notified on this.
///
/// Users may also send a second `SIGINT` signal to force shutdown. In this case, this function
/// will exit the process with a non-zero exit code.
///
/// When `f` returns, this function will assume that the component has finished its work and it's
/// safe to exit. Therefore, this function will exit the process with exit code 0 **without**
/// waiting for background tasks to finish. In other words, it's the responsibility of `f` to
/// ensure that all essential background tasks are finished before returning.
///
/// # Environment variables
///
/// Currently, the following environment variables will be read and used to configure the runtime.
///
/// * `JUICE_WORKER_THREADS` (alias of `TOKIO_WORKER_THREADS`): number of tokio worker threads. If
///   not set, it will be decided by tokio. Note that this can still be overridden by per-module
///   runtime worker thread settings in the config file.
/// * `JUICE_DEADLOCK_DETECTION`: whether to enable deadlock detection. If not set, will enable in
///   debug mode, and disable in release mode.
/// * `JUICE_PROFILE_PATH`: the path to generate flamegraph. If set, then profiling is automatically
///   enabled.
pub fn main_okk<F, Fut>(f: F) -> !
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    set_panic_hook();

    rustls::crypto::ring::default_provider()
        .install_default()
        .inspect_err(|e| {
            tracing::error!(?e, "Failed to install default crypto provider.");
        })
        .unwrap();

    // `TOKIO` will be read by tokio. Duplicate `RW` for compatibility.
    if let Some(worker_threads) = std::env::var_os("JUICE_WORKER_THREADS") {
        unsafe { std::env::set_var("TOKIO_WORKER_THREADS", worker_threads) };
    }

    // Set the default number of worker threads to be at least `MIN_WORKER_THREADS`, in production.
    if !cfg!(debug_assertions) {
        let worker_threads = match std::env::var("TOKIO_WORKER_THREADS") {
            Ok(v) => v
                .parse::<usize>()
                .expect("Failed to parse TOKIO_WORKER_THREADS"),
            Err(std::env::VarError::NotPresent) => std::thread::available_parallelism()
                .expect("Failed to get available parallelism")
                .get(),
            Err(_) => panic!("Failed to parse TOKIO_WORKER_THREADS"),
        };
        if worker_threads < MIN_WORKER_THREADS {
            tracing::warn!(
                "the default number of worker threads ({worker_threads}) is too small, which may lead to issues, increasing to {MIN_WORKER_THREADS}"
            );
            unsafe {
                std::env::set_var("TOKIO_WORKER_THREADS", MIN_WORKER_THREADS.to_string());
            }
        }
    }

    if let Ok(enable_deadlock_detection) = std::env::var("JUICE_DEADLOCK_DETECTION") {
        let enable_deadlock_detection = enable_deadlock_detection
            .parse()
            .expect("Failed to parse RW_DEADLOCK_DETECTION");
        if enable_deadlock_detection {
            enable_parking_lot_deadlock_detection();
        }
    } else {
        // In case the env variable is not set
        if cfg!(debug_assertions) {
            enable_parking_lot_deadlock_detection();
        }
    }

    if let Ok(profile_path) = std::env::var("JUICE_PROFILE_PATH") {
        spawn_prof_thread(profile_path);
    }

    let future_with_shutdown = async move {
        let shutdown = CancellationToken::new();
        let mut fut = pin!(f(shutdown.clone()));

        let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
        let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();

        tokio::select! {
            biased;

            // Watch SIGINT, typically originating from user pressing ctrl-c.
            // Attempt to shutdown gracefully and force shutdown on the next signal.
            _ = sigint.recv() => {
                tracing::info!("received ctrl-c, shutting down... (press ctrl-c again to force shutdown)");
                shutdown.cancel();

                // While waiting for the future to finish, listen for the second ctrl-c signal.
                tokio::select! {
                    biased;
                    _ = sigint.recv() => {
                        tracing::warn!("forced shutdown");

                        // Directly exit the process **here** instead of returning from the future, since
                        // we don't even want to run destructors but only exit as soon as possible.
                        ExitCode::FAILURE.exit_process();
                    }
                    _ = &mut fut => {},
                }
            }

            // Watch SIGTERM, typically originating from Kubernetes.
            // Attempt to shutdown gracefully. No need to force shutdown since it will send SIGKILL after a timeout.
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down...");
                shutdown.cancel();
                fut.await;
            }

            // Proceed with the future.
            _ = &mut fut => {},
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("juicefs-main")
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(future_with_shutdown);

    // Shutdown the runtime and exit the process, without waiting for background tasks to finish.
    // See the doc on this function for more details.
    // TODO(shutdown): is it necessary to shutdown here as we're going to exit?
    runtime.shutdown_background();
    ExitCode::SUCCESS.exit_process();
}
