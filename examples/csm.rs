//! Simple counter state machine example.
//!
//! Demonstrates the basic PHASM pattern with the simplified 2024 async syntax.

use phasm::{
    Input, StateMachine,
    actions::{Action, ActionsContainer, TrackedActionTypes},
};

#[monoio::main]
async fn main() {
    let mut csm = CounterStateMachine { counter: 0 };
    let mut actions = Vec::new();

    CounterStateMachine::stf(&mut csm, Input::Normal(()), &mut actions)
        .await
        .unwrap();

    assert_eq!(
        actions,
        vec![Action::Untracked(CsmAction::Incremented { from: 0, to: 1 })]
    );

    for action in actions.iter() {
        match action {
            Action::Tracked(_) => unreachable!(),
            Action::Untracked(act) => match act {
                CsmAction::Incremented { from, to } => {
                    println!("Incremented from {} to {}", from, to);
                }
            },
        }
    }

    actions.clear();
}

struct CounterStateMachine {
    counter: u64,
}

#[derive(Debug)]
enum CsmError {
    Overflowed,
    FailedToQueueAction,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CsmAction {
    Incremented { from: u64, to: u64 },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CsmTrackedAction;

impl TrackedActionTypes for CsmTrackedAction {
    type Id = ();
    type Action = ();
    type Result = ();
}

impl StateMachine for CounterStateMachine {
    type State = Self;
    type Input = ();
    type TrackedAction = CsmTrackedAction;
    type UntrackedAction = CsmAction;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type TransitionError = CsmError;
    type RestoreError = ();

    async fn stf<'state, 'actions>(
        state: &'state mut Self::State,
        _input: Input<Self::TrackedAction, Self::Input>,
        actions: &'actions mut Self::Actions,
    ) -> Result<(), Self::TransitionError> {
        let prev = state.counter;
        let new = state.counter.checked_add(1).ok_or(CsmError::Overflowed)?;

        // Fallible operation first
        actions
            .add(Action::Untracked(CsmAction::Incremented {
                from: prev,
                to: new,
            }))
            .map_err(|_| CsmError::FailedToQueueAction)?;

        // Then mutate state
        state.counter = new;

        Ok(())
    }

    async fn restore<'state, 'actions>(
        _state: &'state Self::State,
        _actions: &'actions mut Self::Actions,
    ) -> Result<(), Self::RestoreError> {
        // No tracked actions to restore for this simple example
        Ok(())
    }
}
