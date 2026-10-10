// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `feat(rest): expose OAuth token expiry and regeneration`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, ErrorKind};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use mockito::{Mock, Server, ServerGuard};

async fn config_mock(server: &mut ServerGuard) -> Mock {
    server
        .mock("GET", "/v1/config")
        .with_status(200)
        .with_body(r#"{"overrides": {"warehouse": "s3://iceberg-catalog"}, "defaults": {}}"#)
        .create_async()
        .await
}

async fn token_mock(server: &mut ServerGuard, token: &str, status: usize) -> Mock {
    let body = if status == 200 {
        format!(
            r#"{{"access_token": "{token}", "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400}}"#
        )
    } else {
        r#"{"error": {"message": "invalid client", "type": "NotAuthorizedException", "code": 401}}"#
            .to_string()
    };
    server
        .mock("POST", "/v1/oauth/tokens")
        .with_status(status)
        .with_body(body)
        .expect(1)
        .create_async()
        .await
}

async fn catalog(server: &ServerGuard, props: &[(&str, &str)]) -> RestCatalog {
    let mut props: HashMap<String, String> = props
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    props.insert(REST_CATALOG_PROP_URI.to_string(), server.url());
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("test", props)
        .await
        .unwrap()
}

/// Asserts the catalog's next request carries `token`.
async fn assert_sends_token(server: &mut ServerGuard, catalog: &RestCatalog, token: &str) {
    let namespaces = server
        .mock("GET", "/v1/namespaces")
        .match_header("authorization", format!("Bearer {token}").as_str())
        .with_body(r#"{"namespaces": []}"#)
        .expect(1)
        .create_async()
        .await;
    catalog.list_namespaces(None).await.unwrap();
    namespaces.assert_async().await;
    namespaces.remove_async().await;
}

const CREDENTIAL: (&str, &str) = ("credential", "client1:secret1");

#[tokio::test]
async fn token_expiry_is_measured_from_the_request() {
    let mut server = Server::new_async().await;
    let oauth = token_mock(&mut server, "ey000000000000", 200).await;
    let config = config_mock(&mut server).await;

    let catalog = catalog(&server, &[CREDENTIAL]).await;
    let before = Instant::now();
    let expires_at = catalog.token_expires_at().await.unwrap().unwrap();

    oauth.assert_async().await;
    config.assert_async().await;
    // `expires_in` is 86400 seconds, measured from when the token was requested.
    assert!(expires_at > before + Duration::from_secs(86_399));
    assert!(expires_at <= Instant::now() + Duration::from_secs(86_400));
}

#[tokio::test]
async fn regenerate_token_replaces_the_token() {
    let mut server = Server::new_async().await;
    let first = token_mock(&mut server, "ey000000000000", 200).await;
    let _config = config_mock(&mut server).await;

    let catalog = catalog(&server, &[CREDENTIAL]).await;
    let first_expiry = catalog.token_expires_at().await.unwrap().unwrap();
    first.assert_async().await;
    first.remove_async().await;
    assert_sends_token(&mut server, &catalog, "ey000000000000").await;

    let second = token_mock(&mut server, "ey000000000001", 200).await;
    catalog.regenerate_token().await.unwrap();
    second.assert_async().await;

    assert_sends_token(&mut server, &catalog, "ey000000000001").await;
    assert!(catalog.token_expires_at().await.unwrap().unwrap() > first_expiry);
}

#[tokio::test]
async fn failed_regeneration_keeps_the_token() {
    let mut server = Server::new_async().await;
    let first = token_mock(&mut server, "ey000000000000", 200).await;
    let _config = config_mock(&mut server).await;

    let catalog = catalog(&server, &[CREDENTIAL]).await;
    let expiry = catalog.token_expires_at().await.unwrap();
    first.assert_async().await;
    first.remove_async().await;

    let failing = token_mock(&mut server, "", 401).await;
    assert!(catalog.regenerate_token().await.is_err());
    failing.assert_async().await;

    assert_sends_token(&mut server, &catalog, "ey000000000000").await;
    assert_eq!(catalog.token_expires_at().await.unwrap(), expiry);
}

#[tokio::test]
async fn configured_token_has_no_expiry_and_cannot_regenerate() {
    let mut server = Server::new_async().await;
    let _config = config_mock(&mut server).await;

    let catalog = catalog(&server, &[("token", "configured")]).await;
    assert_eq!(catalog.token_expires_at().await.unwrap(), None);

    let err = catalog.regenerate_token().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::DataInvalid);
    assert_sends_token(&mut server, &catalog, "configured").await;
}
