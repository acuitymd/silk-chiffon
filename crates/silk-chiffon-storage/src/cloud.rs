use std::time::Duration;

use url::Url;

pub(crate) fn parse_endpoint(input: &str) -> Result<Url, String> {
    let endpoint = Url::parse(input).map_err(|error| error.to_string())?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err("endpoint URL scheme must be http or https".to_owned());
    }
    if endpoint.host_str().is_none() {
        return Err("endpoint URL must contain a host".to_owned());
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err("endpoint URL must not contain user information".to_owned());
    }
    if endpoint.query().is_some() || endpoint.fragment().is_some() {
        return Err("endpoint URL must not contain a query or fragment".to_owned());
    }
    Ok(endpoint)
}

pub(crate) fn endpoint_string(endpoint: &Url) -> String {
    endpoint.as_str().trim_end_matches('/').to_owned()
}

pub(crate) fn parse_positive_duration(input: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(input).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(duration)
}

pub(crate) fn validate_bucket_location(location: &crate::Location) -> anyhow::Result<()> {
    let url = location.url();
    anyhow::ensure!(
        url.host_str().is_some(),
        "cloud storage URL requires a bucket"
    );
    anyhow::ensure!(
        url.port().is_none(),
        "cloud storage URL must not contain a port"
    );
    anyhow::ensure!(
        url.query().is_none(),
        "cloud storage URL query parameters are not supported"
    );
    anyhow::ensure!(
        url.fragment().is_none(),
        "cloud storage URL fragments are not supported"
    );
    Ok(())
}
