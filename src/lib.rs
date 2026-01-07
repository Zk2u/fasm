//! # PHASM - Fallible Async State Machines
//!
//! A framework for building deterministic, testable, and crash-recoverable state machines
//! with async operations and fallible state access.
//!
//! ## Core Concept
//!
//! PHASM separates state machine logic from external side effects:
//!
//! - **State Transition Function (STF)**: A deterministic function that reads state and input,
//!   validates transitions, mutates state, and emits action descriptions.
//! - **Actions**: Descriptions of side effects (HTTP calls, notifications, analytics) executed
//!   *after* the STF completes successfully and state is committed.
//! - **State**: Can be in-memory structs, database transactions, or any storage accessed through
//!   the `state` parameter. State mutations are part of the transaction, not side effects.
//! - **Restore**: Rebuilds pending actions from persisted state after crashes.
//!
//! ## Execution Model
//!
//! The runtime executes an STF like this:
//!
//! ```ignore
//! let mut txn = db.begin_transaction().await?;
//! let mut actions = Vec::new();
//!
//! match Machine::stf(&mut txn, input, &mut actions).await {
//!     Ok(()) => {
//!         txn.commit().await?;           // Commit state changes
//!         execute_actions(actions).await; // Then execute side effects
//!     }
//!     Err(e) => {
//!         txn.abort().await;  // Rollback state
//!         actions.clear();    // Discard actions
//!         return Err(e);
//!     }
//! }
//! ```
//!
//! **Key insight**: One STF call = one atomic state transaction. All state operations within
//! the STF are part of that transaction. Actions are only executed after successful commit.
//!
//! ## Atomicity
//!
//! PHASM provides atomicity through two mechanisms:
//!
//! ### Transactional State (Database-backed)
//!
//! If your state is a database transaction, atomicity is provided by the storage layer:
//!
//! ```ignore
//! async fn stf(txn: &mut DbTransaction, input: Input, actions: &mut Actions) -> Result<()> {
//!     let user = txn.get("user:123").await?;     // Part of transaction
//!     txn.set("balance", new_balance).await?;    // Part of transaction
//!     txn.set("pending", request).await?;        // Part of transaction
//!
//!     actions.add(Action::Tracked(...))?;        // Buffered, executed after commit
//!     Ok(())
//!     // If any operation fails, transaction aborts and all changes are rolled back
//! }
//! ```
//!
//! ### In-Memory State
//!
//! For in-memory state not covered by a transaction, you must ensure atomicity manually
//! by performing fallible operations before mutating state:
//!
//! ```ignore
//! async fn stf(state: &mut InMemoryState, input: Input, actions: &mut Actions) -> Result<()> {
//!     // 1. Validation (can fail)
//!     if state.balance < amount {
//!         return Err(InsufficientFunds);
//!     }
//!
//!     // 2. Prepare values (no mutation yet)
//!     let id = state.next_id;
//!
//!     // 3. State mutation (after all validation)
//!     state.next_id += 1;
//!     state.pending.insert(id, request);
//!
//!     // 4. Emit actions
//!     actions.add(Action::Tracked(...))?;
//!     Ok(())
//! }
//! ```
//!
//! ## Critical Invariants
//!
//! 1. **Atomicity**: If STF returns `Err`, state must be unchanged (enforced by transaction
//!    or by careful ordering of operations).
//!
//! 2. **Determinism**: Same state + same input = same output. No randomness, system time,
//!    or external I/O in the STF. All external data comes through `input`.
//!
//! 3. **No Side Effects**: STF only mutates state and emits action *descriptions*. Actual
//!    side effects (HTTP calls, sending emails) happen after commit.
//!
//! 4. **Tracked Actions in State**: Before emitting a tracked action, store enough data
//!    in state that `restore()` can recreate it after a crash.
//!
//! 5. **Restore is Pure**: `restore()` only reads from the state parameter. It cannot
//!    make external queries.
//!
//! ## Example
//!
//! ```ignore
//! use phasm::{StateMachine, Input, actions::{Action, TrackedAction, TrackedActionTypes}};
//!
//! struct MySystem {
//!     balance: u64,
//!     pending: HashMap<u64, Request>,
//!     next_id: u64,
//! }
//!
//! struct MyTracked;
//! impl TrackedActionTypes for MyTracked {
//!     type Id = u64;
//!     type Action = PaymentRequest;
//!     type Result = PaymentResult;
//! }
//!
//! impl StateMachine for MySystem {
//!     type State = Self;
//!     type Input = UserRequest;
//!     type TrackedAction = MyTracked;
//!     type UntrackedAction = Notification;
//!     type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
//!     type TransitionError = MyError;
//!     type RestoreError = ();
//!
//!     async fn stf<'s, 'a>(
//!         state: &'s mut Self::State,
//!         input: Input<Self::TrackedAction, Self::Input>,
//!         actions: &'a mut Self::Actions,
//!     ) -> Result<(), Self::TransitionError> {
//!         match input {
//!             Input::Normal(request) => {
//!                 // Handle user request...
//!             }
//!             Input::TrackedActionCompleted { id, result } => {
//!                 // Handle action completion...
//!             }
//!         }
//!         Ok(())
//!     }
//!
//!     async fn restore<'s, 'a>(
//!         state: &'s Self::State,
//!         actions: &'a mut Self::Actions,
//!     ) -> Result<(), Self::RestoreError> {
//!         for (&id, pending) in &state.pending {
//!             actions.add(Action::Tracked(TrackedAction::new(id, ...)))?;
//!         }
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Testing
//!
//! PHASM enables deterministic simulation testing:
//!
//! ```ignore
//! let mut rng = ChaCha8Rng::seed_from_u64(12345);
//! for _ in 0..10000 {
//!     let input = generate_random_input(&mut rng);
//!     Machine::stf(&mut state, input, &mut actions).await?;
//!     state.check_invariants()?;
//! }
//! // Same seed = same execution = reproducible bugs
//! ```

pub mod actions;

use std::future::Future;

use crate::actions::{ActionsContainer, TrackedActionTypes};

/// Input to the state transition function.
///
/// # Variants
///
/// - [`Input::Normal`]: Regular input from users or external systems.
/// - [`Input::TrackedActionCompleted`]: Result of a tracked action that was previously emitted.
///
/// # Determinism
///
/// All external data (timestamps, random values, API responses) MUST be included in the input.
/// The STF must be a pure function of state and input.
///
/// ```ignore
/// // ❌ WRONG - Non-deterministic
/// async fn stf(state: &mut State, input: Input<..., UserRequest>, ...) {
///     let now = SystemTime::now();  // Non-deterministic!
///     let id = Uuid::new_v4();       // Non-deterministic!
/// }
///
/// // ✅ CORRECT - All external data in input
/// struct TimestampedRequest {
///     request: UserRequest,
///     timestamp: SystemTime,
/// }
///
/// async fn stf(state: &mut State, input: Input<..., TimestampedRequest>, ...) {
///     let TimestampedRequest { request, timestamp } = match input {
///         Input::Normal(req) => req,
///         ...
///     };
///     // Use timestamp from input, not SystemTime::now()
/// }
/// ```
pub enum Input<TA: TrackedActionTypes, T> {
    /// Normal input from external sources (user requests, events, etc.)
    Normal(T),

    /// Result of a previously emitted tracked action.
    TrackedActionCompleted {
        /// The ID of the completed action (matches what was in [`TrackedAction::id`](actions::TrackedAction::id)).
        id: TA::Id,
        /// The result of the action.
        result: TA::Result,
    },
}

/// A deterministic, recoverable async state machine.
///
/// # Overview
///
/// A PHASM state machine is conceptually a function:
///
/// ```text
/// (State, Input) -> Result<(State', Actions), Error>
/// ```
///
/// The STF reads current state and input, validates the transition, updates state atomically,
/// and emits actions describing side effects to perform after commit.
///
/// # Implementing
///
/// Implementations can use `async fn` syntax directly:
///
/// ```ignore
/// impl StateMachine for MyMachine {
///     type State = MyState;
///     type Input = MyInput;
///     type TrackedAction = MyTracked;
///     type UntrackedAction = MyUntracked;
///     type Error = MyError;
///
///     async fn stf<'s, 'a>(
///         state: &'s mut Self::State,
///         input: Input<Self::TrackedAction, Self::Input>,
///         actions: &'a mut Vec<Action<Self::UntrackedAction, Self::TrackedAction>>,
///     ) -> Result<(), Self::Error> {
///         // Your implementation here
///         Ok(())
///     }
///
///     async fn restore<'s, 'a>(
///         state: &'s Self::State,
///         actions: &'a mut Vec<Action<Self::UntrackedAction, Self::TrackedAction>>,
/// ) -> Result<(), Self::RestoreError> {
///         // Rebuild pending actions from state
///         Ok(())
///     }
/// }
/// ```
///
/// # Atomicity Rules
///
/// The caller is responsible for wrapping STF in a transaction:
///
/// 1. Begin state transaction before calling STF
/// 2. If STF returns `Ok` → commit transaction, execute actions
/// 3. If STF returns `Err` → abort transaction, discard actions
///
/// Within the STF:
///
/// - **Transactional state**: All operations are part of the transaction; atomicity is automatic.
/// - **In-memory state**: Perform fallible operations before mutations to ensure rollback safety.
///
/// # Actions
///
/// Actions are collected in a `Vec` and only executed after successful commit. Since actions
/// are discarded on error, you can emit them at any point in the STF.
pub trait StateMachine {
    /// The state being managed.
    ///
    /// This can be an in-memory struct, a database transaction, or any mutable reference
    /// to your application state.
    type State;

    /// Normal input type (user requests, external events, etc.)
    ///
    /// Does not include tracked action results - those come via [`Input::TrackedActionCompleted`].
    type Input;

    /// Tracked action types - retryable, restorable, results fed back to STF.
    ///
    /// See [`TrackedActionTypes`] for details.
    type TrackedAction: TrackedActionTypes;

    /// Untracked action type - fire and forget.
    ///
    /// Use for notifications, logging, analytics, UI updates, etc.
    type UntrackedAction;

    /// Container type for actions emitted by the STF.
    ///
    /// Typically `Vec<Action<Self::UntrackedAction, Self::TrackedAction>>`, but can be
    /// a custom container that supports fallible allocation.
    type Actions: ActionsContainer<Self::UntrackedAction, Self::TrackedAction>;

    /// Error type for state transitions.
    type TransitionError;

    /// Error type for restore operations.
    type RestoreError;

    /// The core state transition function.
    ///
    /// # Parameters
    ///
    /// - `state`: Mutable reference to current state. May be a database transaction.
    /// - `input`: The input triggering this transition.
    /// - `actions`: Container to emit actions into. Cleared by caller after each invocation.
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Transition successful. Caller will commit state and execute actions.
    /// - `Err(e)`: Transition failed. Caller will abort state transaction and discard actions.
    ///
    /// # Atomicity
    ///
    /// If state is transactional (database), atomicity is automatic. For in-memory state,
    /// ensure fallible operations complete before mutating state.
    ///
    /// # Example
    ///
    /// ```ignore
    /// async fn stf<'s, 'a>(
    ///     state: &'s mut Self::State,
    ///     input: Input<Self::TrackedAction, Self::Input>,
    ///     actions: &'a mut Self::Actions,
    /// ) -> Result<(), Self::TransitionError> {
    ///     match input {
    ///         Input::Normal(request) => {
    ///             // Validate
    ///             if !state.can_process(&request) {
    ///                 return Err(MyError::InvalidRequest);
    ///             }
    ///
    ///             // Mutate state
    ///             let id = state.next_id;
    ///             state.next_id += 1;
    ///             state.pending.insert(id, request.clone());
    ///
    ///             // Emit tracked action
    ///             actions.add(Action::Tracked(TrackedAction::new(id, request)))?;
    ///
    ///             Ok(())
    ///         }
    ///         Input::TrackedActionCompleted { id, result } => {
    ///             // Handle completion
    ///             state.pending.remove(&id);
    ///             Ok(())
    ///         }
    ///     }
    /// }
    /// ```
    fn stf<'state, 'actions>(
        state: &'state mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'actions mut Self::Actions,
    ) -> impl Future<Output = Result<(), Self::TransitionError>> + use<'state, 'actions, Self>;

    /// Restore pending tracked actions from state after crash/restart.
    ///
    /// After a crash, the runtime calls `restore()` to rebuild the list of pending
    /// tracked actions that need to be retried or status-checked.
    ///
    /// # Parameters
    ///
    /// - `state`: The restored state (loaded from persistent storage or transaction).
    /// - `actions`: Container to emit restored actions into.
    ///
    /// # Rules
    ///
    /// 1. **Pure function of state**: Cannot make external queries or open connections.
    /// 2. **Deterministic**: Same state always produces the same actions.
    /// 3. **Actions pre-cleared**: The runtime clears the actions container before calling restore.
    ///
    /// # Example
    ///
    /// ```ignore
    /// async fn restore<'s, 'a>(
    ///     state: &'s Self::State,
    ///     actions: &'a mut Self::Actions,
    /// ) -> Result<(), Self::RestoreError> {
    ///     for (&id, pending) in &state.pending_operations {
    ///         match pending.status {
    ///             Status::AwaitingResponse => {
    ///                 // Re-check status with external system
    ///                 actions.add(Action::Tracked(
    ///                     TrackedAction::new(id, CheckStatus { id })
    ///                 ))?;
    ///             }
    ///             Status::NeedsRetry => {
    ///                 // Retry the operation
    ///                 actions.add(Action::Tracked(
    ///                     TrackedAction::new(id, pending.original_request.clone())
    ///                 ))?;
    ///             }
    ///             Status::Completed => {
    ///                 // Already done, skip
    ///             }
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn restore<'state, 'actions>(
        state: &'state Self::State,
        actions: &'actions mut Self::Actions,
    ) -> impl Future<Output = Result<(), Self::RestoreError>> + use<'state, 'actions, Self>;
}
