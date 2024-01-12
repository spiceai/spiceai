fn main() {
    if let Err(err) = spiced::run() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
