use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["./types/partitioning.proto"], &["./types/"])?;
    std::env::var("OUT_DIR").unwrap();
    Ok(())
}
