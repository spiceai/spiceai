use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use lopdf::{Document, LoadOptions, Object};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};

#[cfg(feature = "async")]
use tokio::runtime::Builder;

static IGNORE: &[&[u8]] = &[
    b"Length",
    b"BBox",
    b"FormType",
    b"Matrix",
    b"Type",
    b"XObject",
    b"Subtype",
    b"Filter",
    b"ColorSpace",
    b"Width",
    b"Height",
    b"BitsPerComponent",
    b"Length1",
    b"Length2",
    b"Length3",
    b"PTEX.FileName",
    b"PTEX.PageNumber",
    b"PTEX.InfoDict",
    b"FontDescriptor",
    b"ExtGState",
    b"MediaBox",
    b"Annot",
];

#[derive(Debug, Deserialize, Serialize)]
struct PdfText {
    text: BTreeMap<u32, Vec<String>>, // Key is page number
    errors: Vec<String>,
}

#[derive(Parser, Debug)]
#[clap(
    author,
    version,
    about,
    long_about = "Extract TOC and write to file.",
    arg_required_else_help = true
)]
pub struct Args {
    pub pdf_path: PathBuf,

    /// Optional output directory. If omitted the directory of the PDF file will be used.
    #[clap(short, long)]
    pub output: Option<PathBuf>,

    /// Optional pretty print output.
    #[clap(short, long)]
    pub pretty: bool,

    /// Optional password for encrypted PDFs
    #[clap(long, default_value_t = String::from(""))]
    pub password: String,
}

impl Args {
    pub fn parse_args() -> Self {
        Args::parse()
    }
}

fn filter_func(object_id: (u32, u16), object: &mut Object) -> Option<((u32, u16), Object)> {
    if IGNORE.contains(&object.type_name().unwrap_or_default()) {
        return None;
    }
    if let Ok(d) = object.as_dict_mut() {
        d.remove(b"Producer");
        d.remove(b"ModDate");
        d.remove(b"Creator");
        d.remove(b"ProcSet");
        d.remove(b"Procset");
        d.remove(b"XObject");
        d.remove(b"MediaBox");
        d.remove(b"Annots");
        if d.is_empty() {
            return None;
        }
    }
    Some((object_id, object.to_owned()))
}

#[cfg(not(feature = "async"))]
fn load_pdf<P: AsRef<Path>>(path: P) -> Result<Document, Error> {
    Document::load_with_options(path, LoadOptions::with_filter(filter_func))
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

#[cfg(feature = "async")]
fn load_pdf<P: AsRef<Path>>(path: P) -> Result<Document, Error> {
    Builder::new_current_thread().build().unwrap().block_on(async move {
        Document::load_with_options(path, LoadOptions::with_filter(filter_func))
            .await
            .map_err(|e| Error::other(e.to_string()))
    })
}

fn get_pdf_text(doc: &Document) -> Result<PdfText, Error> {
    let mut pdf_text: PdfText = PdfText {
        text: BTreeMap::new(),
        errors: Vec::new(),
    };
    let pages: Vec<Result<(u32, Vec<String>), Error>> = doc
        .get_pages()
        .into_par_iter()
        .map(
            |(page_num, page_id): (u32, (u32, u16))| -> Result<(u32, Vec<String>), Error> {
                let text = doc.extract_text(&[page_num]).map_err(|e| {
                    Error::other(format!(
                        "Failed to extract text from page {page_num} id={page_id:?}: {e:}"
                    ))
                })?;
                Ok((
                    page_num,
                    text.split('\n')
                        .map(|s| s.trim_end().to_string())
                        .collect::<Vec<String>>(),
                ))
            },
        )
        .collect();
    for page in pages {
        match page {
            Ok((page_num, lines)) => {
                pdf_text.text.insert(page_num, lines);
            }
            Err(e) => {
                pdf_text.errors.push(e.to_string());
            }
        }
    }
    Ok(pdf_text)
}

fn pdf2text<P: AsRef<Path> + Debug>(path: P, output: P, pretty: bool, password: &str) -> Result<(), Error> {
    println!("Load {path:?}");
    let mut doc = load_pdf(&path)?;
    if doc.is_encrypted() {
        doc.decrypt(password)
            .map_err(|_err| Error::new(ErrorKind::InvalidInput, "Failed to decrypt"))?;
    }
    let text = get_pdf_text(&doc)?;
    if !text.errors.is_empty() {
        eprintln!("{path:?} has {} errors:", text.errors.len());
        for error in &text.errors[..text.errors.len().min(10)] {
            eprintln!("{error:?}");
        }
    }
    let data = match pretty {
        true => serde_json::to_string_pretty(&text).unwrap(),
        false => serde_json::to_string(&text).unwrap(),
    };
    println!("Write {output:?}");
    let mut f = File::create(output)?;
    f.write_all(data.as_bytes())?;
    Ok(())
}

fn main() -> Result<(), Error> {
    let args = Args::parse_args();

    let start_time = Instant::now();
    let pdf_path = PathBuf::from(shellexpand::full(args.pdf_path.to_str().unwrap()).unwrap().to_string());
    let output = match args.output {
        Some(o) => o.join(pdf_path.file_name().unwrap()),
        None => args.pdf_path,
    };
    let mut output = PathBuf::from(shellexpand::full(output.to_str().unwrap()).unwrap().to_string());
    output.set_extension("text");
    pdf2text(&pdf_path, &output, args.pretty, &args.password)?;
    println!(
        "Done after {:.1} seconds.",
        Instant::now().duration_since(start_time).as_secs_f64()
    );
    Ok(())
}
