use std::str::FromStr;

pub mod meta;
pub mod storage;

pub enum Resource<T> {
    UnLimited,
    Limmited(T),
}

impl<T> FromStr for Resource<T>
where
    T: FromStr,
{
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "0" {
            Ok(Resource::UnLimited)
        } else {
            let value = s.parse::<T>()?;
            Ok(Resource::Limmited(value))
        }
    }
}
