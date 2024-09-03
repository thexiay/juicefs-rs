use tracing::error;

use crate::api::{Slice, Slices};


impl TryFrom<Vec<String>> for Slices {
    type Error = bincode::Error;

    fn try_from(vals: Vec<String>) -> Result<Self, Self::Error> {
        let mut slices = Vec::with_capacity(vals.len());
        for val in vals {
            let slice = bincode::deserialize(val.as_bytes())?;
            slices.push(slice);
        }
        Ok(Slices(slices))
    }
}
