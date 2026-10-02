//! Pair lifecycle.

use crate::lifecycle::Lifecycle;
use crate::wire::wire_enum;

wire_enum! {
    /// State of a pair of devices.
    pub enum PairState ("pair state") {
        /// Invitation created, not claimed.
        Invited = "invited",
        /// Claimed by a device, waits for approval.
        Claimed = "claimed",
        /// Approved. Devices can exchange messages.
        Active = "active",
        /// Unpaired by a device. Terminal.
        Revoked = "revoked",
        /// Not claimed or approved in time. Terminal.
        Expired = "expired",
    }
}

impl Lifecycle for PairState {
    fn allowed_next(self) -> &'static [Self] {
        match self {
            Self::Invited => &[Self::Claimed, Self::Revoked, Self::Expired],
            Self::Claimed => &[Self::Active, Self::Revoked, Self::Expired],
            Self::Active => &[Self::Revoked],
            Self::Revoked | Self::Expired => &[],
        }
    }
}

impl PairState {
    /// `true` if devices can exchange messages.
    #[must_use]
    pub const fn allows_messages(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::PairState::{self, Active, Claimed, Expired, Invited, Revoked};
    use crate::lifecycle::Lifecycle;
    use crate::lifecycle::testing::{check_lifecycle, check_random_walk, requests};
    use crate::wire::WireEnum;
    use crate::wire::testing::check_wire_enum;

    const SPEC: [(PairState, PairState); 7] = [
        (Invited, Claimed),
        (Invited, Revoked),
        (Invited, Expired),
        (Claimed, Active),
        (Claimed, Revoked),
        (Claimed, Expired),
        (Active, Revoked),
    ];

    #[test]
    fn wire_names_are_stable() {
        let names: Vec<&str> = PairState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            names,
            ["invited", "claimed", "active", "revoked", "expired"]
        );
        check_wire_enum::<PairState>();
    }

    #[test]
    fn transitions_match_the_plan() {
        check_lifecycle(Invited, &SPEC);
    }

    #[test]
    fn only_active_pairs_exchange_messages() {
        for &state in PairState::ALL {
            assert_eq!(state.allows_messages(), state == Active, "{state}");
        }
    }

    #[test]
    fn active_pair_never_expires() {
        assert!(!Active.can_transition_to(Expired));
    }

    proptest! {
        #[test]
        fn random_requests_follow_the_lifecycle(requests in requests::<PairState>()) {
            check_random_walk(Invited, &requests)?;
        }
    }
}
