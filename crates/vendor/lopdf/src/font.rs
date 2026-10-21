/// This struct represents the data of a font.
/// It contains information about the font's bounding box, ascent, descent, cap height, italic angle, and stemV.
/// Reference: https://opensource.adobe.com/dc-acrobat-sdk-docs/pdfstandards/pdfreference1.5_v6.pdf
#[derive(Debug, Clone)]
pub struct FontData {
    /// (Required) The PostScript name of the font. This should be the same as the value of BaseFont in the font or
    /// CIDFont dictionary that refers to this font descriptor.
    pub font_name: String,
    /// (Required) A collection of flags defining various characteristics of the font.
    pub flags: i64,
    /// (Required, except for Type 3 fonts) A rectangle (see Section 3.8.4, “Rectangles”), expressed in the glyph
    /// coordinate system, specifying the font bounding box. This is the smallest rectangle enclosing the shape that
    /// would result if all of the glyphs of the font were placed with their origins coincident and then filled.
    /// Format as: (x_min, y_min, x_max, y_max).
    pub font_bbox: (i64, i64, i64, i64),
    /// (Required) The angle, expressed in degrees counterclockwise from the vertical, of the dominant vertical strokes
    /// of the font. (For example, the 9-o’clock position is 90 degrees, and the 3-o’clock position is –90 degrees.)
    /// The value is negative for fonts that slope to the right, as almost all italic fonts do.
    pub italic_angle: i64,
    /// (Required, except for Type 3 fonts) The maximum height above the baseline reached by glyphs in this font,
    /// excluding the height of glyphs for accentedc haracters.
    pub ascent: i64,
    /// (Required, except for Type 3 fonts) The maximum depth below the baseline reached by glyphs in this font. The
    /// value is a negative number.
    pub descent: i64,
    /// (Required for fonts that have Latin characters, except for Type 3 fonts) The vertical coordinate of the top of
    /// flat capital letters, measured from the baseline.
    pub cap_height: i64,
    /// (Required, except for Type 3 fonts) The thickness, measured horizontally, of the dominant vertical stems of
    /// glyphs in the font.
    pub stem_v: i64,
    /// (Required) The name of a predefined CMap, or a stream containing a CMap program, that maps character codes to
    /// font numbers and CIDs. If the descendant is a Type 2 CIDFont whose associated TrueType font program is not
    /// embedded in the PDF file, the Encoding entry must be a predefined CMap name Read more (page 422): https://opensource.adobe.com/dc-acrobat-sdk-docs/pdfstandards/pdfreference1.5_v6.pdf
    pub encoding: String,
    /// Size of the font data in bytes.
    /// This is used to set the `Length1` key in the font stream dictionary.
    font: Vec<u8>,
}

/// This struct is used to store font metadata extracted from a TrueType Fonts (TTF) file.
/// # Examples
///
/// ```no_run
/// // Read a TrueType Fonts (TTF) file.
/// let font_file = std::fs::read("./SomeFont.ttf").unwrap();
///
/// // Create a new FontData instance.
/// let font_name = "SomeFont".to_string();
/// let font_data = lopdf::FontData::new(&font_file, font_name);
/// ```
///
/// Also provides methods to set various font properties such as bounding box, italic angle, ascent, descent, and stemV.
/// # Examples
///
/// ```no_run
/// let font_file = std::fs::read("./SomeFont.ttf").unwrap();
///
/// // Create a new FontData instance along custome value.
/// let font_data = lopdf::FontData::new(&font_file, "SomeFont".to_string())
///                     .set_stem_v(100)
///                     .set_italic_angle(10);
/// ```
impl FontData {
    /// Create a new `FontData` instance by parsing the provided TTF file.
    /// The TTF file is expected to be in bytes.
    pub fn new(font_file: &[u8], font_name: String) -> Self {
        // Parse the TTF file using ttf_parser crate
        let font = ttf_parser::Face::parse(font_file, 0).expect("Failed to parse font file");

        // Extract font metadata
        // Note: The ttf_parser crate provides methods to get font bounding box, ascent, descent, cap height, italic
        // angle, and stemV.
        let font_bbox = font.global_bounding_box();
        let ascent = font.ascender();
        let descent = font.descender();
        let cap_height = font.capital_height().unwrap_or(ascent);
        let italic_angle = font.italic_angle();
        let flags = 1; // Default flags, can be modified later if needed

        // Calculate stemV based on the font bounding box
        // Reference: https://stackoverflow.com/questions/35485179/stemv-value-of-the-truetype-font
        // The stemV is typically calculated as 13% of the font's bbox width value.
        let stem_v = (font_bbox.width() as f64 * 0.13).round() as i64;

        Self {
            font_name,
            flags,
            font_bbox: (
                font_bbox.x_min as i64,
                font_bbox.y_min as i64,
                font_bbox.x_max as i64,
                font_bbox.y_max as i64,
            ),
            italic_angle: italic_angle.round() as i64,
            ascent: ascent as i64,
            descent: descent as i64,
            cap_height: cap_height as i64,
            stem_v,
            encoding: "WinAnsiEncoding".to_string(), // Default encoding, can be modified later if needed
            font: font_file.to_vec(),
        }
    }

    pub fn set_flags(&mut self, flags: i64) -> &mut Self {
        self.flags = flags;
        self
    }

    pub fn set_font_bbox(&mut self, font_bbox: (i64, i64, i64, i64)) -> &mut Self {
        self.font_bbox = font_bbox;
        self
    }

    pub fn set_italic_angle(&mut self, italic_angle: i64) -> &mut Self {
        self.italic_angle = italic_angle;
        self
    }

    pub fn set_ascent(&mut self, ascent: i64) -> &mut Self {
        self.ascent = ascent;
        self
    }

    pub fn set_descent(&mut self, descent: i64) -> &mut Self {
        self.descent = descent;
        self
    }

    pub fn set_cap_height(&mut self, cap_height: i64) -> &mut Self {
        self.cap_height = cap_height;
        self
    }

    pub fn set_stem_v(&mut self, stem_v: i64) -> &mut Self {
        self.stem_v = stem_v;
        self
    }

    pub fn set_encoding(&mut self, encoding: String) -> &mut Self {
        self.encoding = encoding;
        self
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.font.clone()
    }
}
