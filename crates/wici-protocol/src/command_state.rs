//! Command lifecycle.

use crate::lifecycle::Lifecycle;
use crate::wire_enum;

wire_enum! {
    /// Externally visible state of a command.
    ///
    /// ```
    /// use wici_protocol::{CommandState, Lifecycle, TransitionError};
    ///
    /// let state = CommandState::QueuedLocal.transition_to(CommandState::AcceptedDurable)?;
    /// assert!(state.transition_to(CommandState::QueuedLocal).is_err());
    /// # Ok::<(), TransitionError<CommandState>>(())
    /// ```
    pub enum CommandState ("command state") {
        /// Saved on the sender.
        QueuedLocal = "queued_local",
        /// Committed by the server.
        AcceptedDurable = "accepted_durable",
        /// Held by the receiver, not started.
        ReceivedByRunner = "received_by_runner",
        /// Execution intent recorded, started.
        Running = "running",
        /// Waits for an approval.
        AwaitingApproval = "awaiting_approval",
        /// Done, result saved. Terminal.
        Completed = "completed",
        /// Rejected, expired, or errored. Terminal.
        Failed = "failed",
        /// Cancelled before completion. Terminal.
        Cancelled = "cancelled",
        /// Receiver died mid-run. Never runs again.
        OutcomeUnknown = "outcome_unknown",
    }
}

impl Lifecycle for CommandState {
    fn allowed_next(self) -> &'static [Self] {
        match self {
            Self::QueuedLocal => &[Self::AcceptedDurable, Self::Failed, Self::Cancelled],
            Self::AcceptedDurable => &[Self::ReceivedByRunner, Self::Failed, Self::Cancelled],
            // Back to `AcceptedDurable` only after the receiver lease expires.
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
            // Approve and deny both return to `Running`.
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
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::CommandState::{
        self, AcceptedDurable, AwaitingApproval, Cancelled, Completed, Failed, OutcomeUnknown,
        QueuedLocal, ReceivedByRunner, Running,
    };
    use crate::lifecycle::Lifecycle;
    use crate::lifecycle::testing::{check_lifecycle, check_random_walk, reachable_from, requests};
    use crate::wire::WireEnum;
    use crate::wire::testing::check_wire_enum;

    /// The plan's table, kept apart from `allowed_next` on purpose.
    const SPEC: [(CommandState, CommandState); 21] = [
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
        check_wire_enum::<CommandState>();
    }

    #[test]
    fn transitions_match_the_plan() {
        check_lifecycle(QueuedLocal, &SPEC);
    }

    #[test]
    fn terminal_states_are_completed_failed_cancelled() {
        let terminal: Vec<_> = CommandState::ALL
            .iter()
            .filter(|s| s.is_terminal())
            .collect();
        assert_eq!(terminal, [&Completed, &Failed, &Cancelled]);
    }

    #[test]
    fn outcome_unknown_never_runs_again() {
        let reached = reachable_from(OutcomeUnknown);
        for state in [AcceptedDurable, ReceivedByRunner, Running, AwaitingApproval] {
            assert!(!reached.contains(&state), "{state}");
        }
    }

    #[test]
    fn only_execution_can_become_unknown() {
        for &state in CommandState::ALL {
            let expected = matches!(state, Running | AwaitingApproval);
            assert_eq!(state.can_transition_to(OutcomeUnknown), expected, "{state}");
        }
    }

    #[test]
    fn rejected_transition_names_both_states() {
        let error = Completed.transition_to(Running).unwrap_err();
        assert_eq!((error.from(), error.to()), (Completed, Running));
        assert_eq!(
            error.to_string(),
            "command state transition from `completed` to `running` is not allowed"
        );
    }

    proptest! {
        #[test]
        fn random_requests_follow_the_lifecycle(requests in requests::<CommandState>()) {
            check_random_walk(QueuedLocal, &requests)?;
        }
    }
}
