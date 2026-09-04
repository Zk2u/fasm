# FASM - Fallible Async State Machines

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org)

A Rust framework for building **deterministic, testable, and crash-recoverable** state machines with async operations and fallible state access.

## Why FASM?

Traditional state machines break down in production—race conditions, crashes mid-operation, and bugs that only appear under load. FASM solves this by making correctness **verifiable**:

- 🎯 **Deterministic execution** — Same inputs always produce same outputs
- 🔄 **Crash recovery** — Resume from any failure point automatically
- 🧪 **Simulation testing** — Verify correctness across millions of operations in seconds
- 🔒 **Atomicity** — Transactions succeed completely or leave state unchanged
- 💾 **Flexible state** — In-memory, database transactions, or hybrid

## Quick Example

```rust
use fasm::{Input, StateMachine, actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes}};

struct PaymentSystem {
    balance: u64,
    pending: HashMap<u64, Payment>,
    next_id: u64,
}

struct PaymentTracked;
impl TrackedActionTypes for PaymentTracked {
    type Id = u64;
    type Action = PaymentRequest;
    type Result = PaymentResult;
}

impl StateMachine for PaymentSystem {
    type State = Self;
    type Input = PaymentInput;
    type TrackedAction = PaymentTracked;
    type UntrackedAction = Notification;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type TransitionError = PaymentError;
    type RestoreError = ();

    async fn stf<'s, 'a>(
        state: &'s mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'a mut Self::Actions,
    ) -> Result<(), Self::TransitionError> {
        match input {
            Input::Normal(PaymentInput::Process { amount, user }) => {
                // 1. Validate
                if state.balance < amount {
                    return Err(PaymentError::InsufficientFunds);
                }

                // 2. Prepare (no mutation yet)
                let id = state.next_id;

                // 3. Fallible operations first
                actions.add(Action::Tracked(TrackedAction::new(
                    id,
                    PaymentRequest::Charge { amount },
                )))?;

                // 4. Mutate state (point of no return)
                state.next_id += 1;
                state.pending.insert(id, Payment { amount, user, status: Pending });

                Ok(())
            }
            Input::TrackedActionCompleted { id, result } => {
                let payment = state.pending.get_mut(&id)
                    .ok_or(PaymentError::NotFound)?;

                match result {
                    PaymentResult::Success => {
                        state.balance -= payment.amount;
                        payment.status = Confirmed;
                    }
                    PaymentResult::Failed { reason } => {
                        payment.status = Failed;
                    }
                }
                Ok(())
            }
        }
    }

    async fn restore<'s, 'a>(
        state: &'s Self::State,
        actions: &'a mut Self::Actions,
    ) -> Result<(), Self::RestoreError> {
        for (&id, payment) in &state.pending {
            if payment.status == Pending {
                actions.add(Action::Tracked(TrackedAction::new(
                    id,
                    PaymentRequest::CheckStatus { id },
                ))).map_err(|_| ())?;
            }
        }
        Ok(())
    }
}
```

## How It Works

```
Input (user request, external data)
  │
  ▼
┌─────────────────────────────────────┐
│  State Transition Function (STF)    │
│  ─────────────────────────────────  │
│  • Validates inputs                 │
│  • Mutates state atomically         │
│  • Emits action descriptions        │
└─────────────────────────────────────┘
  │
  ├──► State committed
  │
  ├──► Tracked Actions ──► External Systems ──► Results feed back as Input
  │
  └──► Untracked Actions (fire-and-forget)

After crash: restore(state) → re-emit pending tracked actions
```

## Core Concepts

### State Transition Function (STF)

A deterministic function: `(State, Input) → (State', Actions)`

- Validates inputs and mutates state
- Emits action *descriptions* (not executions)
- Must be atomic: if it returns `Err`, state is unchanged

### Actions

**Tracked Actions**: Results feed back into the STF
- Payment processing, external API calls, background jobs
- Stored in state for crash recovery
- Use when the outcome affects system correctness

**Untracked Actions**: Fire-and-forget
- Logs, metrics, notifications, UI updates
- Not recovered after crashes
- Use when you don't need confirmation

### Restore

After a crash, `restore()` rebuilds pending tracked actions from state:
- Pure function of state (no external queries)
- Runtime clears actions container before calling
- Enables automatic crash recovery

## Atomicity

### Transactional State (Database)

If state is a database transaction, atomicity is automatic:

```rust
async fn stf(txn: &mut DbTransaction, input: Input, actions: &mut Actions) -> Result<()> {
    let user = txn.get("user:123").await?;
    txn.set("balance", new_balance).await?;
    actions.add(Action::Tracked(...))?;
    Ok(())
    // If any operation fails, entire transaction aborts
}
```

### In-Memory State

For in-memory state, order operations carefully:

```rust
async fn stf(state: &mut State, input: Input, actions: &mut Actions) -> Result<()> {
    // 1. Validate (can fail)
    if state.balance < amount {
        return Err(InsufficientFunds);
    }

    // 2. Prepare values (no mutation)
    let id = state.next_id;

    // 3. Fallible operations first
    actions.add(Action::Tracked(...))?;

    // 4. Mutate state last
    state.next_id += 1;
    state.pending.insert(id, ...);

    Ok(())
}
```

## Key Rules

### ✅ Must Do

1. **Validate before mutating** — Check conditions before changing state
2. **Deterministic IDs** — Generate from state counters, not random/time
3. **Store tracked actions in state** — Before emitting, so restore can recreate them
4. **Fallible ops before mutations** — For in-memory state atomicity

### ❌ Must Not Do

1. **No side effects in STF** — No HTTP calls, no new connections
2. **No randomness** — No `rand::random()`, no unseeded RNGs
3. **No system time** — Pass timestamps via Input
4. **No external reads** — Except through `state` parameter

## Testing

Deterministic simulation testing—the killer feature:

```rust
#[test]
async fn test_correctness() {
    let mut rng = ChaCha8Rng::seed_from_u64(12345);
    let mut state = MySystem::new();
    let mut actions = Vec::new();

    for i in 0..100_000 {
        let input = generate_random_input(&mut rng);
        let _ = MySystem::stf(&mut state, input, &mut actions).await;
        actions.clear();

        state.check_invariants()
            .expect(&format!("Invariant violated at iteration {}", i));
    }
    // Same seed = same execution = reproducible bugs
}
```

## Examples

```bash
# Simple counter
cargo run --example csm

# Coffee shop loyalty app with tracked/untracked actions
cargo run --example coffee_shop

# Full booking system with simulation tests
cargo test --package dentist_booking
```

## When to Use FASM

### ✅ Great For
- Payment processing
- Reservation systems
- Workflow engines
- Distributed systems requiring correctness

### ❌ Overkill For
- Simple CRUD apps
- Stateless services
- Prototypes (unless correctness matters)

## Version 0.3

- **Simplified trait** — Uses `async fn` directly (Rust 2024 edition)
- **Cleaner API** — No more manual `Future` implementations required
- **Renamed field** — `Input::TrackedActionCompleted { id, result }` (was `res`)

## Storage backends

The `fasm-storage` trait crate defines a directory-native, ordered async byte
key-value store: every data operation names a directory and a key,
`KvDirNav` lists and removes directories, and `Commit` supplies the atomic
commit boundary.

| crate | where it runs | directories | durability / atomicity | how it is tested |
| --- | --- | --- | --- | --- |
| `fasm-storage-btreemap` | in-process, for tests and simulations | shared flat layout via `FlatEngine` | buffered transaction with rollback on drop | native tests and proptest |
| `fasm-storage-redb` | native | shared flat layout via `FlatEngine` | file-backed, single writer, with commit on a dedicated thread | native tests |
| `fasm-storage-fdb` | native | FoundationDB's native directory layer | FoundationDB cluster with optimistic transactions | CI against a live FDB 7.3 cluster behind the `fdb-storage-tests` feature |
| `fasm-storage-indexeddb` | browser (`wasm32-unknown-unknown`) | the shared `FlatEngine` over an owned snapshot | full snapshot per session or reader; buffered delta applied by one revision-fenced readwrite commit | `wasm-pack test --headless --chrome\|--firefox` |

The `kv_store_tests!` and `kv_nav_tests!` conformance suites are the shared
answer key for all four, run natively via `block_on` and in the browser via
`test_attr`.

## Documentation

- [Core Concepts](docs/01_core_concepts.md)
- [Critical Invariants](docs/02_invariants.md)
- [Performance Guide](docs/03_performance.md)
- [Testing Guide](docs/04_testing.md)
- [Database State](docs/05_database_state.md)

## License

MIT OR Apache-2.0
