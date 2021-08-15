use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;

pub struct File {
    data: Vec<Vec<String>>,
    headers: Vec<String>,
    indexes: Vec<HashMap<u64, Vec<usize>, hash_hasher::HashBuildHasher>>,
}

#[derive(Clone)]
pub struct IndexHandle(pub usize);

impl File {
    pub fn new(data: Vec<Vec<String>>, headers: Vec<String>) -> Self {
        Self {
            data,
            headers,
            indexes: Vec::new(),
        }
    }

    fn column_index(&self, col: &str) -> Option<usize> {
        self.headers.iter().position(|header| header == col)
    }

    fn row_equals(&self, condition: &BTreeMap<&str, String>, row: &[String]) -> bool {
        condition
            .iter()
            .all(|(col, value)| match self.column_index(col) {
                None => false,
                Some(idx) => row[idx] == *value,
            })
    }

    fn add_columns(&self, row: &[String]) -> BTreeMap<String, String> {
        self.headers
            .iter()
            .zip(row)
            .map(|(header, col)| (header.clone(), col.clone()))
            .collect()
    }

    /// Creates an index with the given fields.
    /// Uses seahash to create a hash of the data that is used as the key in a hashmap lookup to
    /// the index of the row in the data.
    fn index_data(
        &self,
        index: Vec<&str>,
    ) -> HashMap<u64, Vec<usize>, hash_hasher::HashBuildHasher> {
        // Get the positions of the fields we are indexing
        let fieldidx = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if index.contains(&col.as_ref()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut index = HashMap::with_capacity_and_hasher(
            self.data.len(),
            hash_hasher::HashBuildHasher::default(),
        );

        for (idx, row) in self.data.iter().enumerate() {
            let mut hash = seahash::SeaHasher::default();
            for idx in &fieldidx {
                hash.write(row[*idx].as_bytes());
                hash.write_u8(0);
            }

            let key = hash.finish();

            let entry = index.entry(key).or_insert(Vec::new());
            entry.push(idx);
        }

        index.shrink_to_fit();

        index
    }

    pub fn find_table_row(
        &self,
        condition: BTreeMap<&str, String>,
        index: Option<IndexHandle>,
    ) -> Result<BTreeMap<String, String>, String> {
        match index {
            None => {
                // No index has been passed so we need to do a Sequential Scan.
                let mut found = self.data.iter().filter_map(|row| {
                    if self.row_equals(&condition, &*row) {
                        Some(self.add_columns(row))
                    } else {
                        None
                    }
                });

                let result = found.next();

                if found.next().is_some() {
                    // More than one row has been found.
                    Err("more than one row found".to_string())
                } else {
                    result.ok_or("no rows found".to_string())
                }
            }
            Some(IndexHandle(handle)) => {
                // The index to use has been passed, we can use this to search the data.
                // We are assuming that the caller has passed an index that represents the fields
                // being passed in the condition.
                let mut hash = seahash::SeaHasher::default();

                for field in self.headers.iter() {
                    match condition.get(field as &str) {
                        Some(value) => {
                            hash.write(value.as_bytes());
                            hash.write_u8(0);
                        }
                        None => (),
                    }
                }

                let key = hash.finish();

                self.indexes[handle]
                    .get(&key)
                    .ok_or("no rows found".to_string())
                    .and_then(|rows| {
                        // Ensure we have exactly one result.
                        if rows.len() == 1 {
                            Ok(self.add_columns(&self.data[rows[0]]))
                        } else if rows.is_empty() {
                            Err("no rows found".to_string())
                        } else {
                            Err(format!("{} rows found", rows.len()))
                        }
                    })
            }
        }
    }

    pub fn add_index(&mut self, fields: Vec<&str>) -> Result<IndexHandle, String> {
        self.indexes.push(self.index_data(fields));

        // The returned index handle is the position of the index in our list of indexes.
        Ok(IndexHandle(self.indexes.len() - 1))
    }
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File {} row(s) {} index(es)",
            self.data.len(),
            self.indexes.len()
        )
    }
}
