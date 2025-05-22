use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

use tokio::runtime::Runtime;

/// A wrapper around [`Runtime`] that shuts down the runtime in the background when dropped.
///
/// This is necessary because directly dropping a nested runtime is not allowed in a parent runtime.
pub struct BackgroundShutdownRuntime(ManuallyDrop<Runtime>);

impl Drop for BackgroundShutdownRuntime {
    fn drop(&mut self) {
        // Safety: The runtime is only dropped once here.
        let runtime = unsafe { ManuallyDrop::take(&mut self.0) };
        runtime.shutdown_background();
    }
}

impl Deref for BackgroundShutdownRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BackgroundShutdownRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Runtime> for BackgroundShutdownRuntime {
    fn from(runtime: Runtime) -> Self {
        Self(ManuallyDrop::new(runtime))
    }
}
