//! Enums with stable wire names.

use std::error::Error;
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serializer};

/// An enum whose variants have stable wire names.
///
/// Never rename an existing wire name.
pub trait WireEnum: Copy + Eq + fmt::Debug + 'static {
    /// Human name of the type, used in errors.
    const LABEL: &'static str;
    /// Every variant, in declaration order.
    const ALL: &'static [Self];

    /// Returns the wire name.
    fn as_str(self) -> &'static str;
}

/// A string is not a known wire name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseWireError {
    label: &'static str,
    value: String,
}

impl ParseWireError {
    /// Maximum kept characters of the rejected input.
    pub const MAX_VALUE_CHARS: usize = 64;

    fn new(label: &'static str, value: &str) -> Self {
        Self {
            label,
            value: value.chars().take(Self::MAX_VALUE_CHARS).collect(),
        }
    }

    /// Returns the rejected input, cut to [`Self::MAX_VALUE_CHARS`].
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for ParseWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown {} `{}`", self.label, self.value)
    }
}

impl Error for ParseWireError {}

/// Parses an exact, case-sensitive wire name.
///
/// # Errors
///
/// Returns [`ParseWireError`] for an unknown name.
pub fn parse<T: WireEnum>(value: &str) -> Result<T, ParseWireError> {
    T::ALL
        .iter()
        .copied()
        .find(|item| item.as_str() == value)
        .ok_or_else(|| ParseWireError::new(T::LABEL, value))
}

#[doc(hidden)]
pub fn serialize<T: WireEnum, S: Serializer>(item: T, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(item.as_str())
}

#[doc(hidden)]
pub fn deserialize<'de, T: WireEnum, D: Deserializer<'de>>(deserializer: D) -> Result<T, D::Error> {
    let value = String::deserialize(deserializer)?;
    parse(&value).map_err(D::Error::custom)
}

/// Defines a [`WireEnum`] with `Display`, `FromStr`, and serde support.
/// The calling crate must depend on `serde`.
#[macro_export]
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident ($label:literal) {
            $( $(#[$vmeta:meta])* $variant:ident = $wire:literal, )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis enum $name {
            $( $(#[$vmeta])* $variant, )+
        }

        impl $crate::wire::WireEnum for $name {
            const LABEL: &'static str = $label;
            const ALL: &'static [Self] = &[$(Self::$variant),+];

            fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str($crate::wire::WireEnum::as_str(*self))
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::wire::ParseWireError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                $crate::wire::parse(value)
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                $crate::wire::serialize(*self, s)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                $crate::wire::deserialize(d)
            }
        }
    };
}

#[cfg(test)]
pub(crate) mod testing {
    //! Checks that every [`WireEnum`] must pass.

    use std::collections::HashSet;

    use super::{ParseWireError, WireEnum};

    /// Asserts unique names, `Display`, `FromStr`, and JSON round trips.
    pub(crate) fn check_wire_enum<T>()
    where
        T: WireEnum + std::fmt::Display + std::str::FromStr<Err = ParseWireError>,
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let names: HashSet<&str> = T::ALL.iter().map(|item| item.as_str()).collect();
        assert_eq!(names.len(), T::ALL.len(), "duplicate wire name");
        for &item in T::ALL {
            let name = item.as_str();
            assert_eq!(item.to_string(), name);
            assert_eq!(name.parse::<T>(), Ok(item));
            let json = serde_json::to_string(&item).unwrap();
            assert_eq!(json, format!("\"{name}\""));
            assert_eq!(serde_json::from_str::<T>(&json).unwrap(), item);
        }
        for bad in ["", " ", "UNKNOWN", "a-b"] {
            let error = bad.parse::<T>().unwrap_err();
            assert_eq!(error.value(), bad);
            assert_eq!(error.to_string(), format!("unknown {} `{bad}`", T::LABEL));
        }
        assert!(serde_json::from_str::<T>("\"UNKNOWN\"").is_err());
        assert!(serde_json::from_str::<T>("1").is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    wire_enum! {
        /// Test enum.
        enum Color ("color") {
            /// Red.
            Red = "red",
            /// Blue.
            Blue = "blue",
        }
    }

    #[test]
    fn wire_enum_contract_holds() {
        testing::check_wire_enum::<Color>();
    }

    #[test]
    fn parse_error_keeps_a_bounded_prefix() {
        let long = "x".repeat(ParseWireError::MAX_VALUE_CHARS * 2);
        let error = long.parse::<Color>().unwrap_err();
        assert_eq!(error.value().len(), ParseWireError::MAX_VALUE_CHARS);
    }
}
