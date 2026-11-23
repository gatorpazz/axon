//! Bearer-token authentication.
//!
//! Tokens are looked up by their SHA-256, so the server holds no plaintext
//! credentials and the lookup does not leak timing about the token itself.
//! Swapping this for OIDC means replacing [`Authenticator::authenticate`] with
//! JWT validation; everything downstream only sees a [`PrincipalId`].

use std::collections::HashMap;

use axon_core::PrincipalId;
use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts},
};

use crate::{
    AppState,
    config::{ConfigError, PrincipalConfig, hash_token},
    error::ApiError,
};

#[derive(Debug, Default)]
pub struct Authenticator {
    by_token_hash: HashMap<[u8; 32], PrincipalId>,
}

impl Authenticator {
    pub fn new(principals: &[PrincipalConfig]) -> Result<Self, ConfigError> {
        let mut by_token_hash = HashMap::new();
        for principal in principals {
            let mut hash = [0u8; 32];
            hex::decode_to_slice(&principal.token_sha256, &mut hash)
                .map_err(|_| ConfigError::TokenHash(principal.id.clone()))?;
            if by_token_hash.insert(hash, principal.id.clone()).is_some() {
                return Err(ConfigError::DuplicateToken);
            }
        }
        Ok(Self { by_token_hash })
    }

    pub fn authenticate(&self, token: &str) -> Option<&PrincipalId> {
        self.by_token_hash.get(&hash_token(token))
    }
}

/// Extractor for the authenticated caller. Handlers that take a `Caller`
/// cannot be reached without valid credentials.
#[derive(Debug, Clone)]
pub struct Caller(pub PrincipalId);

impl FromRequestParts<AppState> for Caller {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let token = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, token)| token.trim())
            .ok_or_else(ApiError::unauthorized)?;
        state
            .auth
            .authenticate(token)
            .cloned()
            .map(Caller)
            .ok_or_else(ApiError::unauthorized)
    }
}
