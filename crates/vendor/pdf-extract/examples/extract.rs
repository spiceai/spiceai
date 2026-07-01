extern crate lopdf;
extern crate pdf_extract;

use lopdf::*;
use pdf_extract::*;
use simple_logger::SimpleLogger;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::path;
use std::path::PathBuf;

fn main() {
    SimpleLogger::new().init().unwrap();
    //let output_kind = "html";
    //let output_kind = "txt";
    //let output_kind = "svg";
    let file = env::args().nth(1).unwrap();
    let output_kind = env::args().nth(2).unwrap_or_else(|| "txt".to_owned());
    println!("{}", file);
    let path = path::Path::new(&file);
    let filename = path.file_name().expect("expected a filename");
    let mut output_file = PathBuf::new();
    output_file.push(filename);
    output_file.set_extension(&output_kind);
    let mut output_file =
        BufWriter::new(File::create(output_file).expect("could not create output"));
    let mut doc = Document::load(path).unwrap();

    print_metadata(&doc);

    let mut output: Box<dyn OutputDev> = match output_kind.as_ref() {
        "txt" => Box::new(PlainTextOutput::new(
            &mut output_file as &mut dyn std::io::Write,
        )),
        "html" => Box::new(HTMLOutput::new(&mut output_file)),
        "svg" => Box::new(SVGOutput::new(&mut output_file)),
        _ => panic!(),
    };

    if doc.is_encrypted() {
        doc.decrypt("");
    }

    output_doc(&doc, output.as_mut());
}
