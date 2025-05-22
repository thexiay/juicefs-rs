use std::env;
use std::ffi::OsStr;

/// Checks whether the environment variable `key` is set to `true` or `1` or `t`.
///
/// Returns `false` if the environment variable is not set, or contains invalid characters.
pub fn env_var_is_true(key: impl AsRef<OsStr>) -> bool {
    env_var_is_true_or(key, false)
}

/// Checks whether the environment variable `key` is set to `true` or `1` or `t`.
///
/// Returns `default` if the environment variable is not set, or contains invalid characters.
pub fn env_var_is_true_or(key: impl AsRef<OsStr>, default: bool) -> bool {
    env::var(key)
        .map(|value| {
            ["1", "t", "true"]
                .iter()
                .any(|&s| value.eq_ignore_ascii_case(s))
        })
        .unwrap_or(default)
}