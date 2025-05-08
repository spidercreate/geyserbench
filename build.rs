use {
    std::{
        env,
        path::PathBuf,
    },
    tonic_build::manual::{Builder, Method, Service},
};

const PROTOC_ENVAR: &str = "PROTOC";
#[inline]
pub fn protoc() -> String {
    protobuf_src::protoc().to_str().unwrap().to_string()
}

#[inline]
pub fn mpath(path: &str) -> String {
    path.to_string()
}

fn main() -> anyhow::Result<()> {
    if env::var(PROTOC_ENVAR).is_err() {
        println!("protoc not found in PATH, attempting to fix");
        env::set_var(PROTOC_ENVAR, protoc());
    }

    tonic_build::configure()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").unwrap()).join("arpc_descriptor.bin"),
        )
        .compile_protos(&[mpath("proto/arpc.proto")], &[mpath("proto")])?;

    Builder::new().compile(&[Service::builder()
        .name("ARPCService")
        .package("arpc")
        .method(
            Method::builder()
                .name("subscribe")
                .route_name("Subscribe")
                .client_streaming()
                .server_streaming()
                .input_type("crate::providers::arpc::proto::SubscribeRequest")
                .output_type("crate::providers::arpc::proto::SubscribeResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build()]);
    Ok(())
}