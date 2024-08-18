pub struct Entry {
	pub id: u32,
	pub perm: u16,
}

pub type Entries = Vec<Entry>;

pub struct Rule {
    owner: u16,
    group: u16,
    mask: u16,
    other: u16,
    named_users: Entries,
    named_groups: Entries,
}