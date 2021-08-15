use std::collections::BTreeMap;

use crate::file::{File, IndexHandle};

pub fn setup(
    size: usize,
) -> (
    File,
    IndexHandle,
    BTreeMap<&'static str, String>,
    BTreeMap<String, String>,
) {
    let mut file = File::new(
        // Data
        (0..size)
            .map(|row| {
                // Add 10 columns
                (0..10)
                    .map(|col| format!("data-{}-{}", col, row))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        // Headers
        (0..10)
            .map(|header| format!("field-{}", header))
            .collect::<Vec<_>>(),
    );

    // Search on the first and last field.
    let index = file.add_index(vec!["field-0", "field-9"]).unwrap();

    let mut condition = BTreeMap::new();
    condition.insert("field-0", format!("data-0-{}", size - 1));
    condition.insert("field-9", format!("data-9-{}", size - 1));

    let result = (0..10)
        .map(|idx| {
            (
                format!("field-{}", idx),
                format!("data-{}-{}", idx, size - 1),
            )
        })
        .collect::<BTreeMap<_, _>>();

    (file, index, condition, result)
}
