use anyhow::Result;
use clap::{Parser, ValueEnum};
use jinterner::{
    BufferPool, HashbrownBuildHasher, IValue, InterningDeserializer, InterningDeserializerMut,
    Jinterners, StdBuildHasher,
};
use serde::Deserializer;
use std::fs::File;
use std::hash::BuildHasher;
use std::io::{BufReader, Read};

fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.dispatch_hash()
}

/// Script to parse a JSON file.
#[derive(Parser)]
struct Cli {
    /// JSON file to parse.
    #[arg(long)]
    file: String,

    /// Hash function to use.
    #[arg(long, value_enum, default_value_t = HashFunction::Std)]
    hash: HashFunction,

    /// Whether to use a concurrent jinterner.
    #[arg(long, default_value_t = false)]
    sync: bool,

    /// Whether to use `simd-json` for parsing instead of `serde_json`.
    #[arg(long, default_value_t = false)]
    simd: bool,
}

/// Hash function.
#[derive(ValueEnum, Clone, Copy)]
enum HashFunction {
    /// Standard library hasher.
    Std,
    /// Hasher used by the hashbrown library.
    Hashbrown,
}

impl Cli {
    fn dispatch_hash(&self) -> Result<()> {
        match self.hash {
            HashFunction::Std => self.dispatch_file::<StdBuildHasher>(),
            HashFunction::Hashbrown => self.dispatch_file::<HashbrownBuildHasher>(),
        }
    }

    fn dispatch_file<H: Default + BuildHasher>(&self) -> Result<()> {
        let f = File::open(&self.file)?;
        let (jinterners, ivalue) = self.dispatch_simd::<H>(f)?;
        println!("Jinterners: {:#?}", jinterners.stats());
        println!("Value = {ivalue:?}");
        Ok(())
    }

    fn dispatch_simd<H: Default + BuildHasher>(
        &self,
        mut input: impl Read,
    ) -> Result<(Jinterners<H>, IValue<H>)> {
        if self.simd {
            let mut data = Vec::new();
            input.read_to_end(&mut data)?;
            let mut json_de = simd_json::Deserializer::from_slice(data.as_mut_slice())?;
            self.dispatch_sync::<H, _>(&mut json_de)
        } else {
            let mut json_de = serde_json::Deserializer::from_reader(BufReader::new(input));
            let res = self.dispatch_sync::<H, _>(&mut json_de)?;
            json_de.end()?;
            Ok(res)
        }
    }

    fn dispatch_sync<'de, H, D>(&self, deserializer: D) -> Result<(Jinterners<H>, IValue<H>)>
    where
        H: Default + BuildHasher,
        D: Deserializer<'de>,
        D::Error: Send + Sync + 'static,
    {
        if self.sync {
            let jinterners = Jinterners::<H>::default();
            let mut buffers = BufferPool::default();
            let de = InterningDeserializer::new(deserializer, &jinterners, &mut buffers);
            let ivalue = de.deserialize()?;
            Ok((jinterners, ivalue))
        } else {
            let mut jinterners = Jinterners::<H>::default();
            let mut buffers = BufferPool::default();
            let de = InterningDeserializerMut::new(deserializer, &mut jinterners, &mut buffers);
            let ivalue = de.deserialize()?;
            Ok((jinterners, ivalue))
        }
    }
}
