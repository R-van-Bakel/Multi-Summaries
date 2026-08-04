use std::{
    fs::File,
    io::{BufWriter, Result, Write},
    path::PathBuf,
};

pub trait RotateWrite: Write {
    fn rotate(&mut self);
}

struct PathTemplate {
    directory: PathBuf,
    file_stem: String,
    extension: String,
}

impl PathTemplate {
    fn new(base_filepath: PathBuf) -> Self {
        // Decompose the base filepath into its components and store them in a PathTemplate
        let parent = base_filepath
            .parent()
            .expect("Failed to get parent directory");
        let stem = base_filepath
            .file_stem()
            .expect("Failed to get file stem")
            .to_str()
            .expect("Failed to convert file stem to string")
            .to_string();
        let maybe_ext = base_filepath.extension().map(|e| {
            e.to_str()
                .expect("Failed to convert file extension to string")
                .to_string()
        });
        let ext = match maybe_ext {
            Some(ref e) => format!(".{}", e),
            None => "".to_string(),
        };
        PathTemplate {
            directory: parent.to_path_buf(),
            file_stem: stem,
            extension: ext,
        }
    }
    fn index_format(&self, index: usize) -> PathBuf {
        self.directory.join(format!("{}_{}{}", self.file_stem, index, self.extension))
    }
}

pub struct RotateBufWriter<W: Write> {
    inner: BufWriter<W>,
    current_index: usize,
    file_path_template: PathTemplate,
}

impl RotateBufWriter<File> {
    pub fn new(base_filepath: PathBuf, current_index: usize) -> Self {
        let file_path_template = PathTemplate::new(base_filepath);
        let first_file_path = file_path_template.index_format(current_index);
        let file = File::create(first_file_path).expect("Failed to create file");

        RotateBufWriter {
            inner: BufWriter::new(file),
            file_path_template,
            current_index,
        }
    }
}

impl Write for RotateBufWriter<File> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

impl RotateWrite for RotateBufWriter<File> {
    fn rotate(&mut self) {
        // Flush the current writer and increment the current index
        self.inner.flush().expect("Failed to flush before rotating");
        self.current_index += 1;

        // Create a new file
        let new_file_path = self.file_path_template.index_format(self.current_index);
        let new_file = File::create(&new_file_path).expect("Failed to create new file");

        // Update the inner writer
        self.inner = BufWriter::new(new_file);
    }
}
