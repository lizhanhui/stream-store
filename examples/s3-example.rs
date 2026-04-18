// PoC: access Tencent Cloud COS using the AWS S3 SDK.
//
// Required config files:
//
// ~/.aws/config:
//   [default]
//   region = ap-guangzhou
//   services = cos
//
//   [profile dev]
//   region = ap-beijing
//   services = cos-bj
//
//   [services cos]
//   s3 =
//       endpoint_url = https://cos.ap-guangzhou.myqcloud.com
//       addressing_style = virtual
//
//   [services cos-bj]
//   s3 =
//       endpoint_url = https://cos.ap-beijing.myqcloud.com
//       addressing_style = virtual
//
// ~/.aws/credentials:
//   [default]
//   aws_access_key_id = <COS_SECRETID>
//   aws_secret_access_key = <COS_SECRETKEY>
//
//   [dev]
//   aws_access_key_id = <DEV_COS_SECRETID>
//   aws_secret_access_key = <DEV_COS_SECRETKEY>
//
// Select profile: AWS_PROFILE=dev cargo run --example s3-example
// Override bucket: COS_BUCKET=my-bucket-12345 cargo run --example s3-example

use aws_sdk_s3 as s3;

#[tokio::main]
async fn main() {
    env_logger::init();
    // endpoint_url, region, and addressing_style come from ~/.aws/config.
    let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "dev".to_string());
    let config = aws_config::from_env().profile_name(&profile).load().await;
    let mut config_builder = s3::config::Builder::from(&config);
    config_builder.set_force_path_style(Some(false));

    let config = config_builder.build();
    let client = s3::Client::from_conf(config);

    let bucket_name =
        std::env::var("COS_BUCKET").unwrap_or_else(|_| "stream-storage-1366919849".to_string());
    let key = "s3-example-test/hello.txt";

    // Put object
    let body = b"hello from s3-example";
    let result = client
        .put_object()
        .bucket(&bucket_name)
        .key(key)
        .body(body.to_vec().into())
        .send()
        .await;
    match result {
        Ok(resp) => println!("put_object: ok, etag={:?}", resp.e_tag()),
        Err(e) => eprintln!("put_object failed: {:?}", e),
    }

    // Get object
    let result = client
        .get_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    match result {
        Ok(resp) => {
            let data = resp.body.collect().await.unwrap().into_bytes();
            println!(
                "get_object: ok, key={}, size={}, body={}",
                key,
                data.len(),
                String::from_utf8_lossy(&data),
            );
        }
        Err(e) => eprintln!("get_object failed: {:?}", e),
    }

    // Delete object
    let result = client
        .delete_object()
        .bucket(&bucket_name)
        .key(key)
        .send()
        .await;
    match result {
        Ok(_) => println!("delete_object: ok, key={}", key),
        Err(e) => eprintln!("delete_object failed: {:?}", e),
    }
}
