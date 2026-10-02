//! Checks of the lifecycle rules from the project plan through the public API.

use std::collections::{HashSet, VecDeque};

use proptest::prelude::*;
use proptest::sample::select;
use wici_protocol::CommandState;

/// Returns every state that a command can reach from `start`, `start` included.
fn reachable_from(start: CommandState) -> HashSet<CommandState> {
    let mut reached = HashSet::from([start]);
    let mut pending = VecDeque::from([start]);
    while let Some(state) = pending.pop_front() {
        for &next in state.allowed_next() {
            if reached.insert(next) {
                pending.push_back(next);
            }
        }
    }
    reached
}

fn any_state() -> impl Strategy<Value = CommandState> {
    select(CommandState::ALL.to_vec())
}

#[test]
fn no_state_transitions_to_itself() {
    for state in CommandState::ALL {
        assert!(!state.can_transition_to(state), "{state} -> {state}");
    }
}

#[test]
fn no_state_transitions_to_queued_local() {
    for state in CommandState::ALL {
        assert!(
            !state.can_transition_to(CommandState::QueuedLocal),
            "{state}"
        );
    }
}

#[test]
fn every_state_is_reachable_from_queued_local() {
    let reached = reachable_from(CommandState::QueuedLocal);
    assert_eq!(reached, CommandState::ALL.into_iter().collect());
}

#[test]
fn every_state_can_reach_a_terminal_state() {
    for state in CommandState::ALL {
        assert!(
            reachable_from(state).iter().any(|s| s.is_terminal()),
            "{state} has no path to a terminal state"
        );
    }
}

#[test]
fn outcome_unknown_never_leads_to_execution_again() {
    let reached = reachable_from(CommandState::OutcomeUnknown);
    for execution in [
        CommandState::AcceptedDurable,
        CommandState::ReceivedByRunner,
        CommandState::Running,
        CommandState::AwaitingApproval,
    ] {
        assert!(
            !reached.contains(&execution),
            "outcome_unknown reaches {execution}"
        );
    }
}

#[test]
fn only_execution_states_can_become_outcome_unknown() {
    for state in CommandState::ALL {
        let expected = matches!(
            state,
            CommandState::Running | CommandState::AwaitingApproval
        );
        assert_eq!(
            state.can_transition_to(CommandState::OutcomeUnknown),
            expected,
            "{state}"
        );
    }
}

#[test]
fn every_non_terminal_state_except_outcome_unknown_can_be_cancelled() {
    for state in CommandState::ALL {
        let expected = !state.is_terminal() && state != CommandState::OutcomeUnknown;
        assert_eq!(
            state.can_transition_to(CommandState::Cancelled),
            expected,
            "{state}"
        );
    }
}

proptest! {
    /// A random sequence of requests changes the state only through allowed
    /// transitions, and a terminal state never changes.
    #[test]
    fn random_requests_follow_the_lifecycle(
        requests in proptest::collection::vec(any_state(), 0..64)
    ) {
        let mut state = CommandState::QueuedLocal;
        for next in requests {
            let was_terminal = state.is_terminal();
            match state.transition_to(next) {
                Ok(new_state) => {
                    prop_assert!(!was_terminal);
                    prop_assert_eq!(new_state, next);
                    state = new_state;
                }
                Err(error) => {
                    prop_assert!(!state.can_transition_to(next));
                    prop_assert_eq!((error.from(), error.to()), (state, next));
                }
            }
        }
    }

    /// Every wire name parses back to the same state.
    #[test]
    fn wire_names_round_trip(state in any_state()) {
        prop_assert_eq!(state.as_str().parse::<CommandState>(), Ok(state));
    }

    /// Random text never parses unless it is an exact wire name.
    #[test]
    fn random_text_parses_only_as_a_wire_name(text in ".{0,80}") {
        let known = CommandState::ALL.iter().any(|s| s.as_str() == text);
        prop_assert_eq!(text.parse::<CommandState>().is_ok(), known);
    }
}
