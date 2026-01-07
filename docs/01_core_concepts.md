# PHASM Core Concepts

## What is PHASM?

PHASM (Fallible Async State Machines) is a framework for building **deterministic, testable, and crash-recoverable** state machines with async operations and fallible state access.

## The Problem PHASM Solves

Traditional state machines assume:
- State is in-memory and instantly accessible
- Transitions are synchronous and infallible
- Side effects happen during transitions

In real systems:
- State might be in a database (fallible, async)
- External systems must be called (async, can fail)
- Crashes happen mid-transition

PHASM addresses these by:
1. Making fallibility and async first-class
2. Separating state mutations from external side effects
3. Enabling deterministic testing and crash recovery

## Core Architecture

```
Input → STF → (Updated State, Actions)
         ↓
    Actions executed externally
         ↓
    Results fed back as Input
```

### Components

#### 1. State

Your application state—can be:
- In-memory struct (HashMap, Vec, custom types)
- Database transaction (accessed via `state` parameter)
- Any storage accessed through the `state` parameter

**Rule**: Must be recoverable after crash (persisted or reconstructible).

**Important**: Mutations to state (including database writes through `state`) are NOT side effects—they're the core state transition. Only operations outside of `state` are side effects.

#### 2. Input

Two types:
- **Normal Input**: User requests, external events, timers
- **Tracked Action Results**: Results from previously emitted tracked actions

**Rule**: ALL external data must come through Input:
- ✅ Reading/writing database through `state` parameter: **Allowed**
- ❌ Opening new database connections in STF: **Forbidden**
- ❌ Making HTTP calls to external services: **Forbidden**
- ❌ Reading system time directly: **Forbidden** (pass as input)
- ❌ Using randomness: **Forbidden** (use seeded RNG in state)

#### 3. STF (State Transition Function)

Pure function: `(State, Input) → (State', Actions)`

**Properties**:
- Deterministic: Same state + input = same output
- Atomic: Either succeeds completely or leaves state unchanged
- No external side effects: Only mutates state and emits action descriptions

#### 4. Actions

Descriptions of side effects to execute after commit:

**Tracked Actions**: 
- Require confirmation/results
- Stored in state for crash recovery
- Examples: External API calls, payment processing

**Untracked Actions**:
- Fire-and-forget
- Not recovered after crashes
- Examples: Notifications, logs, metrics

#### 5. Restore

Rebuilds pending tracked actions from state after crash:
- Pure function of state (no external queries)
- Runtime clears actions container before calling
- Enables automatic crash recovery

## Example: Payment Processing

```rust
use phasm::{Input, StateMachine, actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes}};
use std::collections::HashMap;

// State
struct PaymentSystem {
    pending_payments: HashMap<u64, Payment>,
    confirmed_payments: Vec<u64>,
    next_id: u64,
}

struct Payment {
    amount: f32,
    user: String,
    status: PaymentStatus,
    transaction_id: Option<String>,
    failure_reason: Option<String>,
}

#[derive(PartialEq)]
enum PaymentStatus { Pending, Confirmed, Failed }

// Input
enum PaymentInput {
    ProcessPayment { amount: f32, user: String },
}

// Tracked action types
struct PaymentTracked;
impl TrackedActionTypes for PaymentTracked {
    type Id = u64;
    type Action = PaymentAction;
    type Result = PaymentResult;
}

#[derive(Debug, PartialEq, Eq)]
enum PaymentAction {
    ChargeCard { payment_id: u64, amount_cents: u32 },
    CheckStatus { payment_id: u64 },
}

#[derive(Debug)]
enum PaymentResult {
    Success { transaction_id: String },
    Failed { reason: String },
}

// Untracked actions
#[derive(Debug)]
enum Notification {
    NotifyUser { user: String, message: String },
}

// Errors
#[derive(Debug)]
enum PaymentError {
    NotFound,
    ActionFailed,
}

impl StateMachine for PaymentSystem {
    type State = Self;
    type Input = PaymentInput;
    type TrackedAction = PaymentTracked;
    type UntrackedAction = Notification;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type Error = PaymentError;

    async fn stf<'s, 'a>(
        state: &'s mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'a mut Self::Actions,
    ) -> Result<(), Self::Error> {
        match input {
            Input::Normal(PaymentInput::ProcessPayment { amount, user }) => {
                // 1. Prepare values (no mutation yet)
                let payment_id = state.next_id;

                // 2. Fallible operations first
                actions
                    .add(Action::Tracked(TrackedAction::new(
                        payment_id,
                        PaymentAction::ChargeCard {
                            payment_id,
                            amount_cents: (amount * 100.0) as u32,
                        },
                    )))
                    .map_err(|_| PaymentError::ActionFailed)?;

                actions
                    .add(Action::Untracked(Notification::NotifyUser {
                        user: user.clone(),
                        message: "Processing payment...".into(),
                    }))
                    .map_err(|_| PaymentError::ActionFailed)?;

                // 3. Now mutate state
                state.next_id += 1;
                state.pending_payments.insert(
                    payment_id,
                    Payment {
                        amount,
                        user,
                        status: PaymentStatus::Pending,
                        transaction_id: None,
                        failure_reason: None,
                    },
                );

                Ok(())
            }

            Input::TrackedActionCompleted { id: payment_id, result } => {
                let payment = state
                    .pending_payments
                    .get_mut(&payment_id)
                    .ok_or(PaymentError::NotFound)?;

                match result {
                    PaymentResult::Success { transaction_id } => {
                        payment.status = PaymentStatus::Confirmed;
                        payment.transaction_id = Some(transaction_id);
                        state.confirmed_payments.push(payment_id);

                        actions
                            .add(Action::Untracked(Notification::NotifyUser {
                                user: payment.user.clone(),
                                message: "Payment confirmed!".into(),
                            }))
                            .map_err(|_| PaymentError::ActionFailed)?;
                    }
                    PaymentResult::Failed { reason } => {
                        payment.status = PaymentStatus::Failed;
                        payment.failure_reason = Some(reason);
                    }
                }

                Ok(())
            }
        }
    }

    async fn restore<'s, 'a>(
        state: &'s Self::State,
        actions: &'a mut Self::Actions,
    ) -> Result<(), Self::Error> {
        // Runtime clears actions before calling restore
        for (&payment_id, payment) in &state.pending_payments {
            if payment.status == PaymentStatus::Pending {
                actions
                    .add(Action::Tracked(TrackedAction::new(
                        payment_id,
                        PaymentAction::CheckStatus { payment_id },
                    )))
                    .map_err(|_| PaymentError::ActionFailed)?;
            }
        }

        Ok(())
    }
}
```

## Crash Recovery Flow

1. System crashes with pending payment
2. On restart, load state from disk:
   ```rust
   PaymentSystem {
       pending_payments: { 123: Payment { status: Pending, ... } },
       next_id: 124,
   }
   ```
3. Runtime clears actions, calls `restore()`
4. Restore sees payment 123 is pending, emits `CheckStatus(123)`
5. Execute action, get result from external system
6. Feed result back through `stf()` as `TrackedActionCompleted`
7. Payment marked confirmed or failed

## Why This Works

**Determinism**: If payment 123 is pending in state, restore ALWAYS emits the same action.

**Atomicity**: If STF fails before storing in `pending_payments`, the tracked action is never emitted.

**Testability**: Can simulate crash at any point and verify recovery works correctly.

## The Key Insight

By separating **state mutations** from **external side effects** (actions):

1. **Determinism**: STF is a testable pure function
2. **Crash Recovery**: Tracked actions stored in state
3. **Flexibility**: Execute actions however you want
4. **Testability**: Simulate millions of transitions
5. **Clear Boundaries**: State vs. external calls are explicit

## Next Steps

- [Critical Invariants](02_invariants.md) — Rules for correctness
- [Testing Guide](04_testing.md) — Simulation and property testing
- [Database State](05_database_state.md) — Using databases as state