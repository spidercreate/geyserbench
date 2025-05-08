use ::{ std::{ env, path::PathBuf }, tonic_build::manual::{ Builder, Method, Service } };

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

    // Define all proto files
    let proto_files = [
        mpath("proto/arpc.proto"),
        mpath("proto/events.proto"),
        mpath("proto/publisher.proto"),
    ];

    // Compile all proto files
    tonic_build
        ::configure()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptors.bin")
        )
        .compile_protos(&proto_files, &[mpath("proto")])?;

    Builder::new().compile(
        &[
            Service::builder()
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
                        .build()
                )
                .build(),
        ]
    );

    // Define EventPublisher service from publisher.proto
    Builder::new().compile(
        &[
            Service::builder()
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
                        .build()
                )
                .method(
                    Method::builder()
                        .name("subscribe_to_account_updates")
                        .route_name("SubscribeToAccountUpdates")
                        .server_streaming()
                        .input_type("crate::publisher::Empty")
                        .output_type("crate::publisher::StreamResponse")
                        .codec_path("tonic::codec::ProstCodec")
                        .build()
                )
                .method(
                    Method::builder()
                        .name("subscribe_to_slot_status")
                        .route_name("SubscribeToSlotStatus")
                        .server_streaming()
                        .input_type("crate::publisher::Empty")
                        .output_type("crate::publisher::StreamResponse")
                        .codec_path("tonic::codec::ProstCodec")
                        .build()
                )
                .method(
                    Method::builder()
                        .name("subscribe_to_wallet_transactions")
                        .route_name("SubscribeToWalletTransactions")
                        .server_streaming()
                        .input_type("crate::publisher::SubscribeWalletRequest")
                        .output_type("crate::publisher::StreamResponse")
                        .codec_path("tonic::codec::ProstCodec")
                        .build()
                )
                .build(),
        ]
    );
    Ok(())
}
