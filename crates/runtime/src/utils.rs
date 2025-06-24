pub(crate) fn find_io_error<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a std::io::Error> {
    let mut source = Some(err);
    while let Some(e) = source {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }
        source = e.source();
    }
    None
}
