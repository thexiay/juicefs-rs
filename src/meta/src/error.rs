use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum MyError {
    
}

pub type Result<T> = std::result::Result<T, MyError>;