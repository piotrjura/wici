//! State machines with explicit allowed transitions.

use std::error::Error;
use std::fmt;

use crate::wire::WireEnum;

/// A state machine. [`Lifecycle::allowed_next`] is its only rule source.
pub trait Lifecycle: WireEnum {
    /// States reachable in one step. Empty for terminal states.
    fn allowed_next(self) -> &'static [Self];

    /// `true` if the state can never change again.
    fn is_terminal(self) -> bool {
        self.allowed_next().is_empty()
    }

    /// `true` if `self` → `next` is allowed.
    fn can_transition_to(self, next: Self) -> bool {
        self.allowed_next().contains(&next)
    }

    /// Returns `next` if `self` → `next` is allowed.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] if the transition is not allowed.
    fn transition_to(self, next: Self) -> Result<Self, TransitionError<Self>> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(TransitionError {
                from: self,
                to: next,
            })
        }
    }
}

/// A transition that the lifecycle does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransitionError<S> {
    from: S,
    to: S,
}

impl<S: Copy> TransitionError<S> {
    /// State before the rejected transition.
    #[must_use]
    pub const fn from(&self) -> S {
        self.from
    }

    /// Requested state.
    #[must_use]
    pub const fn to(&self) -> S {
        self.to
    }
}

impl<S: WireEnum> fmt::Display for TransitionError<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} transition from `{}` to `{}` is not allowed",
            S::LABEL,
            self.from.as_str(),
            self.to.as_str()
        )
    }
}

impl<S: WireEnum> Error for TransitionError<S> {}

#[cfg(test)]
pub(crate) mod testing {
    //! Checks that every [`Lifecycle`] must pass.

    use std::collections::{HashSet, VecDeque};

    use proptest::prelude::*;

    use super::Lifecycle;

    /// Every state reachable from `start`, `start` included.
    pub(crate) fn reachable_from<S: Lifecycle + std::hash::Hash>(start: S) -> HashSet<S> {
        let mut reached = HashSet::from([start]);
        let mut pending = VecDeque::from([start]);
        while let Some(state) = pending.pop_front() {
            let new: Vec<S> = state
                .allowed_next()
                .iter()
                .copied()
                .filter(|&next| reached.insert(next))
                .collect();
            pending.extend(new);
        }
        reached
    }

    /// Asserts the transition table equals `spec` and the shared invariants:
    /// no self-transitions, nothing returns to `initial`, every state is
    /// reachable from `initial`, and every state can reach a terminal state.
    pub(crate) fn check_lifecycle<S: Lifecycle + std::hash::Hash>(initial: S, spec: &[(S, S)]) {
        let specified: HashSet<(S, S)> = spec.iter().copied().collect();
        assert_eq!(specified.len(), spec.len(), "duplicate spec entry");
        for &from in S::ALL {
            for &to in S::ALL {
                let allowed = from.can_transition_to(to);
                assert_eq!(
                    allowed,
                    specified.contains(&(from, to)),
                    "{from:?} -> {to:?}"
                );
                assert_eq!(from.transition_to(to).is_ok(), allowed);
            }
            assert!(!from.can_transition_to(from), "{from:?} -> itself");
            assert!(!from.can_transition_to(initial), "{from:?} -> initial");
            assert!(
                reachable_from(from).iter().any(|s| s.is_terminal()),
                "{from:?} cannot terminate"
            );
        }
        let all: HashSet<S> = S::ALL.iter().copied().collect();
        assert_eq!(reachable_from(initial), all, "unreachable state");
    }

    /// Applies random requests from `initial` and asserts that only allowed
    /// transitions change the state and that terminal states never change.
    pub(crate) fn check_random_walk<S: Lifecycle>(
        initial: S,
        requests: &[S],
    ) -> Result<(), TestCaseError> {
        let mut state = initial;
        for &next in requests {
            match state.transition_to(next) {
                Ok(new_state) => {
                    prop_assert!(!state.is_terminal());
                    prop_assert_eq!(new_state, next);
                    state = new_state;
                }
                Err(error) => {
                    prop_assert!(!state.can_transition_to(next));
                    prop_assert_eq!((error.from(), error.to()), (state, next));
                }
            }
        }
        Ok(())
    }

    /// Strategy for random request sequences.
    pub(crate) fn requests<S: Lifecycle>() -> impl Strategy<Value = Vec<S>> {
        proptest::collection::vec(proptest::sample::select(S::ALL), 0..64)
    }
}
