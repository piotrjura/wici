//! Command lifecycle states and the rules for transitions between them.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

/// The externally visible state of a command.
///
/// The wire name of each state (see [`CommandState::as_str`]) is part of the
/// protocol. Never change an existing wire name.
///
/// # Examples
///
/// ```
/// use wici_protocol::CommandState;
///
/// let state = CommandState::QueuedLocal.transition_to(CommandState::AcceptedDurable)?;
/// assert_eq!(state, CommandState::AcceptedDurable);
/// assert!(state.transition_to(CommandState::QueuedLocal).is_err());
/// # Ok::<(), wici_protocol::TransitionError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandState {
    /// Saved on the client, not yet accepted by the server.
    QueuedLocal,
    /// Committed by the server.
    AcceptedDurable,
    /// A runner has the command, execution has not started.
    ReceivedByRunner,
    /// The runner recorded the execution intent and started.
    Running,
    /// The agent waits for an approval decision.
    AwaitingApproval,
    /// Finished, the result is saved. Terminal.
    Completed,
    /// Rejected, expired, or finished with an error. Terminal.
    Failed,
    /// Stopped by a cancel request before completion. Terminal.
    Cancelled,
    /// The runner stopped during execution, the effect is unknown.
    ///
    /// Only reconciliation or a user decision resolves this state. Wici never
    /// returns such a command to [`CommandState::Running`].
    OutcomeUnknown,
}

impl CommandState {
    /// Every state, in lifecycle order.
    pub const ALL: [Self; 9] = [
        Self::QueuedLocal,
        Self::AcceptedDurable,
        Self::ReceivedByRunner,
        Self::Running,
        Self::AwaitingApproval,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::OutcomeUnknown,
    ];

    /// Returns the stable wire name of the state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueuedLocal => "queued_local",
            Self::AcceptedDurable => "accepted_durable",
            Self::ReceivedByRunner => "received_by_runner",
            Self::Running => "running",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    /// Returns the states that this state can transition to.
    ///
    /// This is the single definition of the lifecycle rules. The list is empty
    /// for terminal states.
    #[must_use]
    pub const fn allowed_next(self) -> &'static [Self] {
        match self {
            Self::QueuedLocal => &[Self::AcceptedDurable, Self::Failed, Self::Cancelled],
            Self::AcceptedDurable => &[Self::ReceivedByRunner, Self::Failed, Self::Cancelled],
            // Back to `AcceptedDurable` only after the runner lease expires.
            Self::ReceivedByRunner => &[
                Self::Running,
                Self::AcceptedDurable,
                Self::Failed,
                Self::Cancelled,
            ],
            Self::Running => &[
                Self::AwaitingApproval,
                Self::Completed,
                Self::Failed,
                Self::Cancelled,
                Self::OutcomeUnknown,
            ],
            // Approve and deny both return the command to `Running`.
            Self::AwaitingApproval => &[
                Self::Running,
                Self::Failed,
                Self::Cancelled,
                Self::OutcomeUnknown,
            ],
            Self::OutcomeUnknown => &[Self::Completed, Self::Failed],
            Self::Completed | Self::Failed | Self::Cancelled => &[],
        }
    }

    /// Returns `true` if the command can never change state again.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        self.allowed_next().is_empty()
    }

    /// Returns `true` if the lifecycle allows a transition to `next`.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self.allowed_next().contains(&next)
    }

    /// Returns `next` if the lifecycle allows the transition.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] if the lifecycle does not allow a transition
    /// from `self` to `next`. The state does not change.
    pub fn transition_to(self, next: Self) -> Result<Self, TransitionError> {
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

impl fmt::Display for CommandState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CommandState {
    type Err = ParseCommandStateError;

    /// Parses a wire name. The match is exact and case-sensitive.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str() == value)
            .ok_or_else(|| ParseCommandStateError::new(value))
    }
}

/// The lifecycle does not allow a transition between two states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransitionError {
    from: CommandState,
    to: CommandState,
}

impl TransitionError {
    /// Returns the state before the rejected transition.
    #[must_use]
    pub const fn from(&self) -> CommandState {
        self.from
    }

    /// Returns the requested state of the rejected transition.
    #[must_use]
    pub const fn to(&self) -> CommandState {
        self.to
    }
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "command state transition from `{}` to `{}` is not allowed",
            self.from, self.to
        )
    }
}

impl Error for TransitionError {}

/// A string is not a known command state wire name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCommandStateError {
    value: String,
}

impl ParseCommandStateError {
    /// The maximum number of characters of the input that the error keeps.
    pub const MAX_VALUE_CHARS: usize = 64;

    fn new(value: &str) -> Self {
        Self {
            value: value.chars().take(Self::MAX_VALUE_CHARS).collect(),
        }
    }

    /// Returns the rejected input, cut to [`Self::MAX_VALUE_CHARS`] characters.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for ParseCommandStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown command state `{}`", self.value)
    }
}

impl Error for ParseCommandStateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use CommandState::{
        AcceptedDurable, AwaitingApproval, Cancelled, Completed, Failed, OutcomeUnknown,
        QueuedLocal, ReceivedByRunner, Running,
    };

    /// The lifecycle from the project plan, written independently of
    /// `allowed_next` so that a change to either one fails this test.
    const SPECIFIED_TRANSITIONS: [(CommandState, CommandState); 21] = [
        (QueuedLocal, AcceptedDurable),
        (QueuedLocal, Failed),
        (QueuedLocal, Cancelled),
        (AcceptedDurable, ReceivedByRunner),
        (AcceptedDurable, Failed),
        (AcceptedDurable, Cancelled),
        (ReceivedByRunner, Running),
        (ReceivedByRunner, AcceptedDurable),
        (ReceivedByRunner, Failed),
        (ReceivedByRunner, Cancelled),
        (Running, AwaitingApproval),
        (Running, Completed),
        (Running, Failed),
        (Running, Cancelled),
        (Running, OutcomeUnknown),
        (AwaitingApproval, Running),
        (AwaitingApproval, Failed),
        (AwaitingApproval, Cancelled),
        (AwaitingApproval, OutcomeUnknown),
        (OutcomeUnknown, Completed),
        (OutcomeUnknown, Failed),
    ];

    #[test]
    fn wire_names_are_stable() {
        let names: Vec<&str> = CommandState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            names,
            [
                "queued_local",
                "accepted_durable",
                "received_by_runner",
                "running",
                "awaiting_approval",
                "completed",
                "failed",
                "cancelled",
                "outcome_unknown",
            ]
        );
    }

    #[test]
    fn all_lists_each_state_once() {
        let unique: HashSet<CommandState> = CommandState::ALL.into_iter().collect();
        assert_eq!(unique.len(), CommandState::ALL.len());
    }

    #[test]
    fn display_writes_the_wire_name() {
        for state in CommandState::ALL {
            assert_eq!(state.to_string(), state.as_str());
        }
    }

    #[test]
    fn parse_accepts_every_wire_name() {
        for state in CommandState::ALL {
            assert_eq!(state.as_str().parse::<CommandState>(), Ok(state));
        }
    }

    #[test]
    fn parse_rejects_unknown_names() {
        for value in [
            "",
            "Running",
            " running",
            "running ",
            "queued-local",
            "done",
        ] {
            let error = value.parse::<CommandState>().unwrap_err();
            assert_eq!(error.value(), value);
            assert_eq!(
                error.to_string(),
                format!("unknown command state `{value}`")
            );
        }
    }

    #[test]
    fn parse_error_limits_the_kept_input() {
        let long = "x".repeat(ParseCommandStateError::MAX_VALUE_CHARS * 2);
        let error = long.parse::<CommandState>().unwrap_err();
        assert_eq!(
            error.value().chars().count(),
            ParseCommandStateError::MAX_VALUE_CHARS
        );
    }

    #[test]
    fn transitions_match_the_specification() {
        let specified: HashSet<_> = SPECIFIED_TRANSITIONS.into_iter().collect();
        for from in CommandState::ALL {
            for to in CommandState::ALL {
                assert_eq!(
                    from.can_transition_to(to),
                    specified.contains(&(from, to)),
                    "transition {from} -> {to}"
                );
            }
        }
    }

    #[test]
    fn terminal_states_are_completed_failed_and_cancelled() {
        let terminal: Vec<CommandState> = CommandState::ALL
            .into_iter()
            .filter(|s| s.is_terminal())
            .collect();
        assert_eq!(terminal, [Completed, Failed, Cancelled]);
    }

    #[test]
    fn allowed_transition_returns_the_next_state() {
        assert_eq!(Running.transition_to(Completed), Ok(Completed));
    }

    #[test]
    fn rejected_transition_reports_both_states() {
        let error = Completed.transition_to(Running).unwrap_err();
        assert_eq!(error.from(), Completed);
        assert_eq!(error.to(), Running);
        assert_eq!(
            error.to_string(),
            "command state transition from `completed` to `running` is not allowed"
        );
    }
}
