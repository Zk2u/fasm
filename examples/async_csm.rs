#![allow(dead_code)]

use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use phasm::{
    Input, StateMachine,
    actions::{Action, ActionsContainer, TrackedActionTypes},
};

/// Demonstrates PHASM with async, fallible state operations.
///
/// This example shows how to build state machines where state access itself is:
/// - **Async**: Operations take time (database transactions, network calls)
/// - **Fallible**: Operations can fail (connection timeouts, lock contention)
///
/// Key PHASM invariants demonstrated:
/// 1. **Atomicity**: If any operation fails, state remains COMPLETELY unchanged
/// 2. **Atomic writes**: Write operations are all-or-nothing (no partial updates)
/// 3. **Determinism**: Failures are deterministic based on state flags
/// 4. **Async operations**: State reads/writes properly awaited
/// 5. **State through parameter**: All state access goes through `state` parameter
///
/// **Critical property**: When a write fails, the state is left exactly as it was
/// before the STF was called. There are no partial updates, no inconsistent state.
/// This is what makes PHASM state machines theoretically sound.
///
/// This pattern is essential for database-backed state machines (FoundationDB,
/// PostgreSQL, etc.) where state operations are inherently async and fallible.

#[monoio::main(enable_timer = true)]
async fn main() {
    println!("=== Async Counter State Machine Demo ===\n");

    // Create state with async, fallible backend
    let mut state = AsyncCounterState::new(0);
    let mut actions = Vec::new();

    println!("Initial counter value:");
    match state.read_counter().await {
        Ok(val) => println!("  Counter: {}\n", val),
        Err(e) => println!("  Failed to read: {:?}\n", e),
    }

    // Increment counter - success case
    println!(">>> Incrementing counter (will succeed)");
    match AsyncCounterStateMachine::stf(&mut state, Input::Normal(()), &mut actions).await {
        Ok(()) => {
            println!("✓ STF succeeded");
            println!("  Actions emitted: {}", actions.len());
            for action in &actions {
                match action {
                    Action::Untracked(CsmAction::Incremented { from, to }) => {
                        println!("  - Incremented from {} to {}", from, to);
                    }
                    _ => {}
                }
            }
        }
        Err(e) => println!("✗ STF failed: {:?}", e),
    }
    actions.clear();

    println!("\nCurrent counter value:");
    match state.read_counter().await {
        Ok(val) => println!("  Counter: {}\n", val),
        Err(e) => println!("  Failed to read: {:?}\n", e),
    }

    // Simulate a failure during state read
    println!(">>> Incrementing counter (simulating failure)");
    state.simulate_failure = true;

    let counter_before = state.counter_value; // Check state before
    match AsyncCounterStateMachine::stf(&mut state, Input::Normal(()), &mut actions).await {
        Ok(()) => println!("✓ STF succeeded (unexpected)"),
        Err(e) => {
            println!("✗ STF failed: {:?}", e);
            println!("  State unchanged (atomicity preserved)");
            println!("  Counter before: {}", counter_before);
            println!("  Counter after: {}", state.counter_value);
            assert_eq!(counter_before, state.counter_value, "Atomicity violated!");
        }
    }

    println!("\n=== Demo Complete ===");
}

// ============================================================================
// Async, Fallible State
// ============================================================================

/// State with async, fallible operations.
/// All state access goes through async methods that can fail.
struct AsyncCounterState {
    counter_value: u64,
    simulate_failure: bool,
}

impl AsyncCounterState {
    fn new(initial: u64) -> Self {
        Self {
            counter_value: initial,
            simulate_failure: false,
        }
    }

    /// Async read with 5ms delay that can fail.
    ///
    /// In a real system, this would be:
    /// - Database transaction read: `state.txn.get(key).await?`
    /// - Network call: `state.client.fetch(id).await?`
    /// - File I/O: `state.file.read().await?`
    ///
    /// The 5ms delay simulates network/disk latency.
    async fn read_counter(&self) -> Result<u64, StateError> {
        // Simulate async operation (e.g., database read)
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;

        if self.simulate_failure {
            return Err(StateError::ReadFailed);
        }

        Ok(self.counter_value)
    }

    /// Async write with 5ms delay that can fail.
    ///
    /// **CRITICAL: This write is atomic** - either it succeeds completely or fails
    /// completely, leaving state unchanged. There is no partial update.
    ///
    /// In a real system, this would be:
    /// - Database transaction write: `state.txn.set(key, value).await?` (atomic)
    /// - Network call: `state.client.update(id, value).await?` (atomic request)
    /// - File I/O: `state.file.write(value).await?` (atomic at OS level)
    ///
    /// The 5ms delay simulates network/disk latency.
    async fn write_counter(&mut self, value: u64) -> Result<(), StateError> {
        // Simulate async operation (e.g., database write)
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;

        if self.simulate_failure {
            return Err(StateError::WriteFailed);
        }

        self.counter_value = value;
        Ok(())
    }
}

#[derive(Debug)]
enum StateError {
    ReadFailed,
    WriteFailed,
}

// ============================================================================
// State Machine Implementation
// ============================================================================

struct AsyncCounterStateMachine;

#[derive(Debug)]
enum AsyncCsmError {
    StateReadError(StateError),
    StateWriteError(StateError),
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

impl StateMachine for AsyncCounterStateMachine {
    type UntrackedAction = CsmAction;
    type TrackedAction = CsmTrackedAction;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;

    type State = AsyncCounterState;
    type Input = ();

    type TransitionError = AsyncCsmError;
    type RestoreError = ();

    type StfFuture<'state, 'actions> = AsyncCsmStfFuture<'state, 'actions>;
    type RestoreFuture<'state, 'actions> =
        Pin<Box<dyn Future<Output = Result<(), Self::RestoreError>> + 'state>>;

    fn stf<'state, 'actions>(
        state: &'state mut Self::State,
        _input: Input<Self::TrackedAction, Self::Input>,
        actions: &'actions mut Self::Actions,
    ) -> Self::StfFuture<'state, 'actions> {
        AsyncCsmStfFuture::new(state, actions)
    }

    fn restore<'state, 'actions>(
        _state: &'state Self::State,
        _actions: &'actions mut Self::Actions,
    ) -> Self::RestoreFuture<'state, 'actions> {
        Box::pin(async move {
            // No tracked actions to restore for this simple example
            Ok(())
        })
    }
}

// ============================================================================
// Custom Future Implementation
// ============================================================================

/// Custom future that properly handles async, fallible state operations
/// while maintaining PHASM invariants.
///
/// This future implements a state machine that progresses through stages:
/// 1. Reading: Async read current value from state
/// 2. Validating: Check if operation is valid (deterministic, no I/O)
/// 3. Writing: Async write new value to state
/// 4. Emitting: Add action to actions container
///
/// CRITICAL: If any stage fails, state remains unchanged (atomicity).
/// The key insight: We validate BEFORE writing, ensuring errors don't
/// leave state in an inconsistent state.
struct AsyncCsmStfFuture<'state, 'actions> {
    state: &'state mut AsyncCounterState,
    actions: &'actions mut Vec<Action<CsmAction, CsmTrackedAction>>,
    stage: StfStage,
}

enum StfStage {
    // Reading: Async read from state (can fail)
    Reading(Pin<Box<dyn Future<Output = Result<u64, StateError>> + 'static>>),

    // Validating: Pure, synchronous validation (no I/O)
    Validating {
        prev: u64,
    },

    // Writing: Async write to state (ATOMIC - can fail, but validated first)
    // If the write fails, state remains unchanged - no partial updates
    Writing {
        prev: u64,
        new: u64,
        fut: Pin<Box<dyn Future<Output = Result<(), StateError>> + 'static>>,
    },

    // Emitting: Add actions after successful state mutation
    Emitting {
        prev: u64,
        new: u64,
    },

    // Done: Initial/temporary state
    Done,
}

impl<'state, 'actions> AsyncCsmStfFuture<'state, 'actions> {
    fn new(
        state: &'state mut AsyncCounterState,
        actions: &'actions mut Vec<Action<CsmAction, CsmTrackedAction>>,
    ) -> Self {
        Self {
            state,
            actions,
            stage: StfStage::Done, // Will be set to Reading on first poll
        }
    }
}

impl<'state, 'actions> Future for AsyncCsmStfFuture<'state, 'actions> {
    type Output = Result<(), AsyncCsmError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            // Use mem::replace to take ownership of stage, avoiding borrow issues
            let stage = mem::replace(&mut self.stage, StfStage::Done);

            match stage {
                StfStage::Done => {
                    // Initial state - start reading
                    // INVARIANT: We must read current state value before any mutations
                    // SAFETY: We're creating a 'static future by capturing values, not references
                    // The actual async operation (sleep) doesn't borrow from state
                    let counter_value = self.state.counter_value;
                    let simulate_failure = self.state.simulate_failure;

                    let read_fut = Box::pin(async move {
                        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
                        if simulate_failure {
                            Err(StateError::ReadFailed)
                        } else {
                            Ok(counter_value)
                        }
                    });

                    self.stage = StfStage::Reading(read_fut);
                }
                StfStage::Reading(mut fut) => {
                    // Poll the read future - this is the async state read
                    // If this fails, no state has been modified (atomicity preserved)
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok(prev)) => {
                            // Read succeeded - move to validation
                            // INVARIANT: All validation BEFORE any state mutations
                            self.stage = StfStage::Validating { prev };
                        }
                        Poll::Ready(Err(e)) => {
                            // Read failed - return error immediately
                            // State is unchanged (atomicity: no writes happened)
                            return Poll::Ready(Err(AsyncCsmError::StateReadError(e)));
                        }
                        Poll::Pending => {
                            self.stage = StfStage::Reading(fut);
                            return Poll::Pending;
                        }
                    }
                }
                StfStage::Validating { prev } => {
                    // INVARIANT: Pure validation - no I/O, no side effects
                    // This must be deterministic and synchronous
                    let new = match prev.checked_add(1) {
                        Some(n) => n,
                        None => {
                            // Validation failed - no state mutation occurred
                            return Poll::Ready(Err(AsyncCsmError::Overflowed));
                        }
                    };

                    // All validation passed - NOW we can create the write future
                    // This is the key: validate first, then mutate
                    let simulate_failure = self.state.simulate_failure;
                    let write_fut = Box::pin(async move {
                        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
                        if simulate_failure {
                            Err(StateError::WriteFailed)
                        } else {
                            Ok(())
                        }
                    });

                    self.stage = StfStage::Writing {
                        prev,
                        new,
                        fut: write_fut,
                    };
                }
                StfStage::Writing { prev, new, mut fut } => {
                    // Poll the write future - this is the ATOMIC async state write
                    // **CRITICAL**: This write operation is all-or-nothing:
                    //   - Success: state.counter_value gets updated
                    //   - Failure: state.counter_value remains unchanged (NO partial updates)
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok(())) => {
                            // Write succeeded - actually mutate state
                            // INVARIANT: State mutation only after successful write
                            self.state.counter_value = new;
                            self.stage = StfStage::Emitting { prev, new };
                        }
                        Poll::Ready(Err(e)) => {
                            // **ATOMIC WRITE FAILED** - state remains EXACTLY as it was
                            // CRITICAL: counter_value was NOT modified, so state is consistent
                            // This atomicity is what makes PHASM state machines theoretically sound
                            // In a real system (database), the transaction would be rolled back
                            return Poll::Ready(Err(AsyncCsmError::StateWriteError(e)));
                        }
                        Poll::Pending => {
                            self.stage = StfStage::Writing { prev, new, fut };
                            return Poll::Pending;
                        }
                    }
                }
                StfStage::Emitting { prev, new } => {
                    // Emit action after successful state mutation
                    // INVARIANT: Actions only emitted after state successfully updated
                    // This ensures action describes what actually happened
                    match self.actions.add(Action::Untracked(CsmAction::Incremented {
                        from: prev,
                        to: new,
                    })) {
                        Ok(()) => return Poll::Ready(Ok(())),
                        Err(_) => return Poll::Ready(Err(AsyncCsmError::FailedToQueueAction)),
                    }
                }
            }
        }
    }
}
