use ntex_redis::{cmd, Client, RedisConnector};
use ntex_redis::cmd::stream::XInfoType;
use ntex_redis::errors::CommandError;


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

#[ntex::test]
async fn test_xgroup() -> Result<(), CommandError> {
  let client = connect().await;
  let err_sub_cmd_not_set
      = cmd::stream::XGroup("cube.test", "test_group").build();
  match err_sub_cmd_not_set {
    Err(e) => println!("xgroup error: {:?}", e),
    Ok(v) => println!("xgroup full: {:?}", v)
  };
  let xgroup_create = client.exec(cmd::stream::XGroup("cube.test", "test_group1").create().build()?).await;
  match xgroup_create {
    Err(e) => println!("xgroup create error: {:?}", e),
    Ok(v) => println!("xgroup create reply: {:?}", v)
  };
  let xgroup_createconsumer = client
      .exec(cmd::stream::XGroup("cube.test", "test_group").create_consumer("test1").build()?).await;
  match xgroup_createconsumer {
    Err(e) => println!("xgroup create consumer error: {:?}", e),
    Ok(v) => println!("xgroup create consumer reply: {:?}", v)
  };
  let xgroup_delteconsumer = client
      .exec(cmd::stream::XGroup("cube.test", "test_group").delete_consumer("test1").build()?).await;
  match xgroup_delteconsumer {
    Err(e) => println!("xgroup delete consumer error: {:?}", e),
    Ok(v) => println!("xgroup delete consumer reply: {:?}", v)
  };
  let xgroup_destroy = client.exec(cmd::stream::XGroup("cube.test", "test_group1").destroy().build()?).await;
  match xgroup_destroy {
    Err(e) => println!("xgroup destroy error: {:?}", e),
    Ok(v) => println!("xgroup destroy reply: {:?}", v)
  };
  Ok(())
}
