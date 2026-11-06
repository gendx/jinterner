use anyhow::{Context, Result, bail};
use jinterner::{IValue, Jinterners};
use rstest::rstest;
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{self, PathBuf};

#[rstest]
fn test_intern(#[files("data/**/*.json")] mut path: PathBuf) -> Result<()> {
    if !path.set_extension("") {
        bail!("Failed to remove extension from {path:?}");
    }
    let name = path
        .strip_prefix(
            path::absolute("data/").with_context(|| "Failed to find absolute path for data/")?,
        )
        .with_context(|| format!("Failed to remove directory prefix from {path:?}"))?
        .to_str()
        .with_context(|| format!("Failed to convert path to string for {path:?}"))?;

    let file = BufReader::new(
        File::open(format!("data/{name}.json"))
            .with_context(|| format!("Failed to read file data/{name}.json"))?,
    );
    let mut jinterners = Jinterners::default();
    let ivalue = IValue::from_json_reader_mut(file, &mut jinterners)
        .with_context(|| format!("Failed to deserialize file data/{name}.json"))?;

    let serialized = postcard::to_stdvec(&InternedFile { jinterners, ivalue })
        .with_context(|| format!("Failed to serialize {name} as postcard"))?;

    let mut f = File::open(format!("data/{name}.postcard"))
        .with_context(|| format!("Failed to read file data/{name}.postcard"))?;
    let mut expected = Vec::new();
    f.read_to_end(&mut expected)
        .with_context(|| format!("Failed to read file data/{name}.postcard"))?;
    assert_eq!(serialized, expected);

    Ok(())
}

#[rstest]
fn test_optimize(#[files("data/**/*.json")] mut path: PathBuf) -> Result<()> {
    if !path.set_extension("") {
        bail!("Failed to remove extension from {path:?}");
    }
    let name = path
        .strip_prefix(
            path::absolute("data/").with_context(|| "Failed to find absolute path for data/")?,
        )
        .with_context(|| format!("Failed to remove directory prefix from {path:?}"))?
        .to_str()
        .with_context(|| format!("Failed to convert path to string for {path:?}"))?;

    let file = BufReader::new(
        File::open(format!("data/{name}.json"))
            .with_context(|| format!("Failed to read file data/{name}.json"))?,
    );
    let mut jinterners = Jinterners::default();
    let ivalue = IValue::from_json_reader_mut(file, &mut jinterners)
        .with_context(|| format!("Failed to deserialize file data/{name}.json"))?;

    let (jinterners, ivalue) = match jinterners.optimize(None) {
        None => (jinterners, ivalue),
        Some((jinterners, mapping)) => (jinterners, mapping.map(ivalue)),
    };

    let serialized = postcard::to_stdvec(&InternedFile { jinterners, ivalue })
        .with_context(|| format!("Failed to serialize {name} as postcard"))?;

    let mut f = File::open(format!("data/{name}.opt.postcard"))
        .with_context(|| format!("Failed to read file data/{name}.opt.postcard"))?;
    let mut expected = Vec::new();
    f.read_to_end(&mut expected)
        .with_context(|| format!("Failed to read file data/{name}.opt.postcard"))?;
    assert_eq!(serialized, expected);

    Ok(())
}

#[derive(Serialize)]
struct InternedFile {
    jinterners: Jinterners,
    ivalue: IValue,
}
