//! Client tests against a real server and PostgreSQL. Run through
//! `scripts/with-postgres.sh`.

#[cfg(test)]
mod flows;
#[cfg(test)]
mod keepalive;
#[cfg(test)]
mod shutdown;
#[cfg(test)]
mod support;
