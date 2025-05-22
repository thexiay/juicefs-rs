/// Set panic hook to abort the process if we're not catching unwind, without losing the information
/// of stack trace and await-tree.
pub fn set_panic_hook() {
    if let Ok(limit) = rlimit::Resource::CORE.get_soft()
        && limit > 0
    {
        tracing::info!(limit, "coredump on panic is likely to be enabled");
    };

    std::panic::update_hook(|default_hook, info| {
        default_hook(info);

        if let Some(context) = await_tree::current_tree() {
            println!("\n\n*** await tree context of current task ***\n");
            println!("{}\n", context);
        }
    });
}
