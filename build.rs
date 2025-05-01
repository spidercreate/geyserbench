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

    // Define all proto files
    let proto_files = [
        mpath("proto/arpc.proto"),
        mpath("proto/events.proto"),
        mpath("proto/publisher.proto"),
    ];
    
    // Compile all proto files
    tonic_build::configure()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptors.bin"),
        )
        .compile_protos(&proto_files, &[mpath("proto")])?;

    // Define ARPCService
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
        
    // Define EventPublisher service from publisher.proto
    Builder::new().compile(&[Service::builder()
        .name("EventPublisher")
        .package("publisher")
        .method(
            Method::builder()
                .name("subscribe_to_transactions")
                .route_name("SubscribeToTransactions")
                .server_streaming()
                .input_type("crate::publisher::Empty")
                .output_type("crate::publisher::StreamResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("subscribe_to_account_updates")
                .route_name("SubscribeToAccountUpdates")
                .server_streaming()
                .input_type("crate::publisher::Empty")
                .output_type("crate::publisher::StreamResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("subscribe_to_slot_status")
                .route_name("SubscribeToSlotStatus")
                .server_streaming()
                .input_type("crate::publisher::Empty")
                .output_type("crate::publisher::StreamResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("subscribe_to_wallet_transactions")
                .route_name("SubscribeToWalletTransactions")
                .server_streaming()
                .input_type("crate::publisher::SubscribeWalletRequest")
                .output_type("crate::publisher::StreamResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build()]);

    Ok(())
}
