use rusqlite::types::FromSql;
use rusqlite::Row;

use crate::db::FromRow;

fn inc(n: &mut usize) -> usize {
    let v = *n;
    (*n) = v + 1;
    v
}

macro_rules! derive_tuple {
    ($($field:ident)*) => {
        impl<$($field: FromSql,)*> FromRow for ($($field,)*) {
            fn from_row(row: &Row) -> rusqlite::Result<Self> {
                let mut cursor = 0;
                Ok((
                    $(
                        {
                            let _name = stringify!($field);
                            row.get(inc(&mut cursor))?
                        },
                    )*
                ))
            }
        }
    };
}

macro_rules! derive_common_tuples {
    () => {};
    ($first:ident $($rest:ident)*) => {
        derive_tuple!($first $($rest)*);
        derive_common_tuples!($($rest)*);
    };
}

derive_common_tuples!(T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15 T16);
