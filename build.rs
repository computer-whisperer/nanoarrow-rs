fn main() -> anyhow::Result<()> {
    femtopb_build::compile_protos(
        &["src/protocol/Flight.proto"],
        &["src"],
    )
}