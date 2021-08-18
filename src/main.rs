use std::time::{Duration, Instant};

mod file;
mod setup;

fn main() {
    time_size(10);
    time_size(1_000);
    time_size(100_000);
    time_size(1_000_000);
}

fn time_size(size: usize) {
    println!("Size {}", size);
    println!("-------------");

    let (file, index, condition, expected) = setup::setup(size);
    let (_, duration) = time(|| {
        assert_eq!(
            Ok(expected),
            file.find_table_row(condition.clone(), Some(index.clone()))
        )
    });

    println!("Indexed");
    println!("Time elapsed is: {:?}", duration);

    let (file, _index, condition, expected) = setup::setup(size);
    let (_, duration) =
        time(|| assert_eq!(Ok(expected), file.find_table_row(condition.clone(), None)));

    println!("");
    println!("Sequential");
    println!("Time elapsed is: {:?}", duration);
    println!("");
}

fn time<T, F: FnOnce() -> T>(func: F) -> (T, Duration) {
    let start = Instant::now();
    let t = func();
    (t, start.elapsed())
}
