use anyhow::{Context, Result};
use criterion::{
    BatchSize, Bencher, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use jinterner::{
    ArrayOrder, BufferPool, HashbrownBuildHasher, IValue, InterningDeserializer,
    InterningDeserializerMut, Jinterners, StdBuildHasher,
};
use serde_json::Value;
use std::fs::File;
use std::hash::BuildHasher;
use std::hint::black_box;
use std::io::Read;

const FILES: &[&str] = &[
    "open_data/idfm_disruptions",
    "open_data/lime_paris_free_bike_status",
    "simd_json/apache_builds",
    "simd_json/canada",
    "simd_json/citm_catalog",
    "simd_json/github_events",
    "simd_json/gsoc-2018",
    "simd_json/instruments",
    "simd_json/marine_ik",
    "simd_json/mesh",
    "simd_json/mesh.pretty",
    "simd_json/numbers",
    "simd_json/random",
    "simd_json/twitterescaped",
    "simd_json/twitter",
    "simd_json/update-center",
];

fn parse(c: &mut Criterion) {
    if let Err(e) = parse_impl(c) {
        panic!("Benchmarking failed: {e:?}");
    }
}

fn parse_impl(c: &mut Criterion) -> Result<()> {
    let mut group = c.benchmark_group("parse");
    for name in FILES {
        let mut file = File::open(format!("data/{name}.json"))
            .with_context(|| format!("Failed to read file data/{name}.json"))?;
        let mut input = Vec::new();
        file.read_to_end(&mut input)
            .with_context(|| format!("Failed to read file data/{name}.json"))?;

        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("serde_json", name),
            input.as_slice(),
            parse_serde_json,
        );
        group.bench_with_input(
            BenchmarkId::new("simd_json", name),
            input.as_slice(),
            parse_simd_json,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner", name),
            input.as_slice(),
            parse_jinterner::<StdBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_mut", name),
            input.as_slice(),
            parse_jinterner_mut::<StdBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_simd", name),
            input.as_slice(),
            parse_jinterner_simd::<StdBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_simd_mut", name),
            input.as_slice(),
            parse_jinterner_simd_mut::<StdBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_hashbrown", name),
            input.as_slice(),
            parse_jinterner::<HashbrownBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_hashbrown_mut", name),
            input.as_slice(),
            parse_jinterner_mut::<HashbrownBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_hashbrown_simd", name),
            input.as_slice(),
            parse_jinterner_simd::<HashbrownBuildHasher>,
        );
        group.bench_with_input(
            BenchmarkId::new("jinterner_hashbrown_simd_mut", name),
            input.as_slice(),
            parse_jinterner_simd_mut::<HashbrownBuildHasher>,
        );
    }
    group.finish();

    Ok(())
}

fn parse_serde_json(bencher: &mut Bencher, input: &[u8]) {
    bencher.iter(|| {
        let v: Value = serde_json::from_slice(input).unwrap();
        v
    });
}

fn parse_simd_json(bencher: &mut Bencher, input: &[u8]) {
    bencher.iter_batched_ref(
        || input.to_vec(),
        |i| {
            let v: Value = simd_json::from_slice(i.as_mut_slice()).unwrap();
            v
        },
        BatchSize::PerIteration,
    );
}

fn parse_jinterner<H: Default + BuildHasher>(bencher: &mut Bencher, input: &[u8]) {
    bencher.iter(|| {
        let jinterners: Jinterners<H> = Jinterners::default();
        IValue::from_json_slice(black_box(input), &jinterners, ArrayOrder::Preserve).unwrap();
    });
}

fn parse_jinterner_mut<H: Default + BuildHasher>(bencher: &mut Bencher, input: &[u8]) {
    bencher.iter(|| {
        let mut jinterners: Jinterners<H> = Jinterners::default();
        IValue::from_json_slice_mut(black_box(input), &mut jinterners, ArrayOrder::Preserve)
            .unwrap()
    });
}

fn parse_jinterner_simd<H: Default + BuildHasher>(bencher: &mut Bencher, input: &[u8]) {
    bencher.iter_batched_ref(
        || input.to_vec(),
        |i| {
            let jinterners: Jinterners<H> = Jinterners::default();
            let mut json_de = simd_json::Deserializer::from_slice(i.as_mut_slice()).unwrap();
            let mut buffers = BufferPool::default();
            let de = InterningDeserializer::new(
                &mut json_de,
                &jinterners,
                &mut buffers,
                ArrayOrder::Preserve,
            );
            de.deserialize().unwrap()
        },
        BatchSize::PerIteration,
    );
}

fn parse_jinterner_simd_mut<H: Default + BuildHasher>(bencher: &mut Bencher, input: &[u8]) {
    bencher.iter_batched_ref(
        || input.to_vec(),
        |i| {
            let mut jinterners: Jinterners<H> = Jinterners::default();
            let mut json_de = simd_json::Deserializer::from_slice(i.as_mut_slice()).unwrap();
            let mut buffers = BufferPool::default();
            let de = InterningDeserializerMut::new(
                &mut json_de,
                &mut jinterners,
                &mut buffers,
                ArrayOrder::Preserve,
            );
            de.deserialize().unwrap()
        },
        BatchSize::PerIteration,
    );
}

criterion_group!(benches, parse);
criterion_main!(benches);
