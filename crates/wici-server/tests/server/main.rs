//! Server tests against real PostgreSQL and SQLite. Run through
//! `scripts/with-postgres.sh`.
#![expect(
    clippy::cognitive_complexity,
    reason = "a contract test walks one flow through many steps"
)]

#[cfg(test)]
mod client;
#[cfg(test)]
mod store_artifacts;
#[cfg(test)]
mod store_messages;
#[cfg(test)]
mod store_pairs;
#[cfg(test)]
mod support;
#[cfg(test)]
mod ws;
