//! HTTP-based connector implementation types.

use apollo_federation::sources::connect::ApplyToError;

use crate::plugins::connectors::make_requests::ResponseKey;
use crate::services::connector::request_service::TransportRequest;

#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) request: TransportRequest,
    pub(crate) key: ResponseKey,
    pub(crate) apply_to_errors: Vec<ApplyToError>,
}
