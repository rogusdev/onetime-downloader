
use std::convert::TryFrom;
//use std::fmt::Display;

use crate::models::MyError;


// https://users.rust-lang.org/t/impl-tryinto-as-an-argument-in-a-function-complains-about-the-error-conversion/34004
pub fn try_from_vec<T, U: TryFrom<T, Error=MyError>> (items: Vec<T>, name: &'static str) -> Result<Vec<U>, MyError>  {
    let mut vec = Vec::new();
    for item in items.into_iter() {
        match U::try_from(item) {
            Err(why) => return Err(format!("Failed converting {}: {}", name, why)),
            Ok(file) => vec.push(file),
        }
    }
    Ok(vec)
}
