use {
    std::{
        env, fs,
        path::{Path, PathBuf},
    },
    tonic_build::manual::{Builder, Method, Service},
};

const PROTOC_ENVAR: &str = "PROTOC";

// NOTE: Also set PROTOC_INCLUDE on Windows! Ex with winget: C:\Users\Astra\AppData\Local\Microsoft\WinGet\Packages\Google.Protobuf_Microsoft.Winget.Source_8wekyb3d8bbwe\include

#[inline]
pub fn protoc() -> String {
    #[cfg(not(windows))]
    return protobuf_src::protoc().to_str().unwrap().to_string();

    #[cfg(windows)]
    powershell_script::run("(Get-Command protoc).Path")
        .unwrap()
        .stdout()
        .unwrap()
        .to_string()
        .trim()
        .replace(r#"\\"#, r#"\"#)
}

#[inline]
pub fn mpath(path: &str) -> String {
    #[cfg(not(windows))]
    return path.to_string();

    #[cfg(windows)]
    return path.replace(r#"\"#, r#"\\"#);
}

fn main() -> anyhow::Result<()> {
    if std::env::var(PROTOC_ENVAR).is_err() {
        println!("protoc not found in PATH, attempting to fix");
        std::env::set_var(PROTOC_ENVAR, protoc());
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
                .input_type("crate::arpc::SubscribeRequest")
                .output_type("crate::arpc::SubscribeResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build()]);

    Ok(())
}
