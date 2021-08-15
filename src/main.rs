use std::time::{Duration, Instant};

mod file;
mod setup;

fn main() {
    let (file, index, condition, _expected) = setup::setup(1_000_000);

    let (result, duration) = time(|| file.find_table_row(condition.clone(), Some(index.clone())));

    println!("Indexed");
    println!("{:?}", result);
    println!("Time elapsed is: {:?}", duration);

    let (result, duration) = time(|| file.find_table_row(condition.clone(), None));

    println!("");
    println!("Sequential");
    println!("{:?}", result);
    println!("Time elapsed is: {:?}", duration);
}

fn time<T, F: FnOnce() -> T>(func: F) -> (T, Duration) {
    let start = Instant::now();
    let t = func();
    (t, start.elapsed())
}
