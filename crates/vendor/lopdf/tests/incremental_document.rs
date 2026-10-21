use lopdf::Result;
use tempfile::tempdir;

mod utils;

#[test]
fn load_incremental_file_as_linear_file() -> Result<()> {
    let doc = utils::load_document("assets/Incremental.pdf")?;
    assert_eq!(doc.version, "1.5".to_string());

    Ok(())
}

#[test]
fn load_incremental_file() -> Result<()> {
    let mut doc = utils::load_incremental_document("assets/Incremental.pdf")?;
    assert_eq!(doc.get_prev_documents().version, "1.5".to_string());

    // Create temporary folder to store file.
    let temp_dir = tempdir()?;
    let file_path = temp_dir.path().join("test_4_incremental.pdf");
    doc.save(file_path)?;

    Ok(())
}
