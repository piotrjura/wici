//! Column decoding and value conversion. Every adapter selects the same
//! column names and types, so one decoder serves all of them.

use sqlx::{ColumnIndex, Decode, Row, Type};
use uuid::Uuid;
use wici_protocol::{DeviceId, Lane, PairId, Position, Timestamp, WireEnum};

use crate::store::{StoreError, StoreResult};

/// A row that decodes a column as `T`.
pub(in crate::store) trait Field<T> {
    fn field(&self, column: &str) -> StoreResult<T>;
}

impl<R, T> Field<T> for R
where
    R: Row,
    for<'c> &'c str: ColumnIndex<R>,
    T: for<'r> Decode<'r, R::Database> + Type<R::Database>,
{
    fn field(&self, column: &str) -> StoreResult<T> {
        Ok(self.try_get(column)?)
    }
}

/// A row that decodes every column type the store uses.
pub(in crate::store) trait Fields:
    Field<Uuid>
    + Field<String>
    + Field<Vec<u8>>
    + Field<Option<Vec<u8>>>
    + Field<i64>
    + Field<Option<i64>>
    + Field<bool>
{
}

impl<R> Fields for R where
    R: Field<Uuid>
        + Field<String>
        + Field<Vec<u8>>
        + Field<Option<Vec<u8>>>
        + Field<i64>
        + Field<Option<i64>>
        + Field<bool>
{
}

/// Column `column` of `row` as `T`.
pub(super) fn get<T>(row: &impl Field<T>, column: &str) -> StoreResult<T> {
    row.field(column)
}

pub(super) fn device_from(bytes: &[u8]) -> StoreResult<DeviceId> {
    <[u8; 32]>::try_from(bytes)
        .map(DeviceId::from_bytes)
        .map_err(|_| StoreError::corrupt("device ID length"))
}

pub(in crate::store) fn timestamp(millis: Option<i64>) -> Option<Timestamp> {
    millis.and_then(|ms| u64::try_from(ms).ok()).map(Timestamp)
}

pub(in crate::store) fn position_from(value: i64) -> StoreResult<Position> {
    u64::try_from(value)
        .map(Position)
        .map_err(|_| StoreError::corrupt("negative position"))
}

pub(in crate::store) fn position_value(position: Position) -> StoreResult<i64> {
    i64::try_from(position.0).map_err(|_| StoreError::Forbidden)
}

pub(super) fn wire_from<T: WireEnum>(text: &str) -> StoreResult<T> {
    wici_protocol::wire::parse(text).map_err(|_| StoreError::corrupt("unknown wire name"))
}

pub(super) fn uuid_bytes(row: &impl Fields, column: &str) -> StoreResult<[u8; 16]> {
    Ok(get::<Uuid>(row, column)?.into_bytes())
}

pub(super) fn pair_id_from(row: &impl Fields, column: &str) -> StoreResult<PairId> {
    Ok(PairId::from_bytes(uuid_bytes(row, column)?))
}

pub(super) fn lane_from(row: &impl Fields, column: &str) -> StoreResult<Lane> {
    wire_from(&get::<String>(row, column)?)
}
