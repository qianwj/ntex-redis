use ntex_redis::{cmd, Client, RedisConnector};
use ntex_redis::cmd::stream::XInfoType;


async fn connect() -> Client {
  RedisConnector::new("127.0.0.1:6379")
    .connect()
    .await
    .unwrap()
}

#[ntex::test]
async fn test_xinfo() {
  let client = connect().await;
  let result = client.exec(cmd::stream::XInfo("cube.test".to_string(), XInfoType::Stream(None)).full()).await;
  match result {
    Err(e) => println!("error: {:?}", e),
    Ok(v) => println!("xinfo stream full: {:?}", v)
  };
  let result2 = client.exec(cmd::stream::XInfo("cube.test".to_string(), XInfoType::Stream(None))).await;
  match result2 {
    Err(e) => println!("error: {:?}", e),
    Ok(v) => println!("xinfo stream: {:?}", v)
  };
  let result3 = client.exec(cmd::stream::XInfo("cube.test".to_string(), XInfoType::Groups)).await;
  match result3 {
    Err(e) => println!("error: {:?}", e),
    Ok(v) => println!("xinfo groups: {:?}", v)
  }
  let result4 = client.exec(cmd::stream::XInfo("cube.test".to_string(), XInfoType::Consumers("test".to_string()))).await;
  match result4 {
    Err(e) => println!("error: {:?}", e),
    Ok(v) => println!("xinfo consumers: {:?}", v)
  }
}
