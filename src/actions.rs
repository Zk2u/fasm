//! Actions emitted by state transition functions.
//!
//! Actions describe side effects to be executed after a successful state transition.
//! They come in two flavors:
//!
//! - **Tracked**: Retryable, restorable, and their results are fed back to the STF.
//! - **Untracked**: Fire-and-forget side effects (notifications, logging, etc.)

use std::fmt::Debug;

/// Defines the types associated with tracked actions.
///
/// Tracked actions are retryable, restorable, and their completion results
/// are fed back into the state machine via [`Input::TrackedActionCompleted`](crate::Input::TrackedActionCompleted).
///
/// # Example
///
/// ```ignore
/// struct PaymentActions;
///
/// impl TrackedActionTypes for PaymentActions {
///     type Id = u64;
///     type Action = PaymentRequest;
///     type Result = PaymentResult;
/// }
/// ```
pub trait TrackedActionTypes {
    /// Identifier for correlating action completion with the original request.
    ///
    /// Must be stored in state so that when the result arrives, the STF knows
    /// which pending operation it corresponds to.
    type Id: Debug + PartialEq + Eq + PartialOrd;

    /// The action payload describing what external operation to perform.
    type Action: Debug + PartialEq + Eq;

    /// The result returned when the action completes (success or failure).
    type Result: Debug;
}

/// A tracked action with its identifier.
///
/// Tracked actions are executed by the runtime after STF completes successfully.
/// Their results are fed back to the STF as [`Input::TrackedActionCompleted`](crate::Input::TrackedActionCompleted).
///
/// # Example
///
/// ```ignore
/// let action = TrackedAction::new(request_id, PaymentRequest::Charge { user_id, amount });
/// actions.add(Action::Tracked(action))?;
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct TrackedAction<Types: TrackedActionTypes> {
    action_id: Types::Id,
    action: Types::Action,
}

impl<Types: TrackedActionTypes> TrackedAction<Types> {
    /// Create a new tracked action.
    pub fn new(action_id: Types::Id, action: Types::Action) -> Self {
        Self { action_id, action }
    }

    /// Get the action ID.
    pub fn id(&self) -> &Types::Id {
        &self.action_id
    }

    /// Get the action payload.
    pub fn action(&self) -> &Types::Action {
        &self.action
    }
}

/// An action emitted by the state transition function.
///
/// Actions are collected during STF execution and executed by the runtime
/// only after the state transition commits successfully.
///
/// # Variants
///
/// - [`Action::Tracked`]: Results are fed back to the STF. Use for operations
///   where you need to know the outcome (payments, external API calls).
///
/// - [`Action::Untracked`]: Fire-and-forget. Use for notifications, logging,
///   analytics, UI updates, etc.
#[derive(Debug, PartialEq, Eq)]
pub enum Action<UA, TATypes: TrackedActionTypes> {
    /// A tracked action whose result will be fed back to the STF.
    Tracked(TrackedAction<TATypes>),

    /// An untracked fire-and-forget action.
    Untracked(UA),
}

/// A trait for describing a fallible container for a set of [`Action`]s.
///
/// This trait exists to support fallible allocation. When Rust supports fallible
/// heap allocations, adding an action may fail if out of memory. The container
/// abstraction allows this to be handled gracefully.
///
/// # Clearing Behavior
///
/// The caller is responsible for clearing the container after each STF invocation,
/// regardless of success or failure. Actions are only executed if the STF succeeds
/// and the state transaction commits.
pub trait ActionsContainer<UA, TA: TrackedActionTypes> {
    /// The error type for container operations.
    type Error;

    /// Creates a new instance of the container.
    ///
    /// May fail if the container cannot be initialized (e.g., allocation failure).
    fn new() -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Creates a new instance of the container with a capacity hint.
    ///
    /// May fail if the container cannot be initialized (e.g., allocation failure).
    fn with_capacity(capacity: usize) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Clears the container, removing all actions.
    ///
    /// May fail if the container cannot be cleared.
    fn clear(&mut self) -> Result<(), Self::Error>;

    /// Adds an action to the container.
    ///
    /// May fail if the container cannot be modified (e.g., allocation failure).
    fn add(&mut self, action: Action<UA, TA>) -> Result<(), Self::Error>;
}

impl<UA, TA: TrackedActionTypes> ActionsContainer<UA, TA> for Vec<Action<UA, TA>> {
    type Error = ();

    fn new() -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Vec::new())
    }

    fn with_capacity(capacity: usize) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Vec::with_capacity(capacity))
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        Vec::clear(self);
        Ok(())
    }

    fn add(&mut self, action: Action<UA, TA>) -> Result<(), Self::Error> {
        self.push(action);
        Ok(())
    }
}
