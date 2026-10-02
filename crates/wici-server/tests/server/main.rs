//! Server tests against a real PostgreSQL. Run through `scripts/with-postgres.sh`.

#[cfg(test)]
mod store_messages;
#[cfg(test)]
mod store_pairs;
#[cfg(test)]
mod support;
