use aws_sdk_s3 as s3;

#[tokio::main]
async fn main() {
    env_logger::init();
    let config = aws_config::from_env().profile_name("default").load().await;
    let mut config_builder = s3::config::Builder::from(&config);
    config_builder.set_force_path_style(Some(false));
    let region = "ap-guangzhou";
    config_builder.set_region(Some(s3::config::Region::new(region)));
    let endpoint_url = "https://stream-storage-1366919849.cos.ap-guangzhou.myqcloud.com";
    config_builder.set_endpoint_url(Some(endpoint_url.into()));

    // config_builder.set_credentials_provider(credentials_provider);

    let config = config_builder.build();

    let client = s3::Client::from_conf(config);

    let result = client.list_buckets().send().await;
    println!("List buckets: {:?}", result);
    if let Ok(resp) = result {
        for bucket in resp.buckets() {
            println!("Bucket: {}", bucket.name().unwrap_or_default());
        }
    }
    println!("Hello, S3!");
}
