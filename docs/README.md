# FASM Documentation

Comprehensive guides for building correct, performant, and testable state machines with FASM.

## Getting Started

1. [Core Concepts](01_core_concepts.md) - Understanding FASM's architecture and design
2. [Critical Invariants](02_invariants.md) - Rules for building sound state machines
3. [Performance Guide](03_performance.md) - Optimizing your state machines
4. [Testing Guide](04_testing.md) - Deterministic simulation testing
5. [Database-Backed State](05_database_state.md) - Using transactional databases as state

## Quick Links

### Core Concepts
- What is FASM and why use it?
- State, Input, STF, Actions, and Restore
- Example: Payment processing system

### Critical Invariants
- STF Atomicity
- Determinism requirements
- State validity
- Tracked action storage
- Restore purity

### Performance
- Data structure selection (ahash, BTreeMap)
- Invariant checking strategies
- Memory efficiency
- In-memory optimization patterns

### Database-Backed State
- State as transactional database (FoundationDB, PostgreSQL)
- STF atomicity via database transactions
- Deterministic IDs from database sequences
- Restore from database state
- Hybrid in-memory + database patterns

### Testing
- Deterministic simulation with seeded RNGs
- Time-bounded test runners
- Property-based testing
- Crash recovery testing
- Race condition testing

## Philosophy

FASM is designed around these principles:

1. **Determinism First**: Same state + input = same output (always)
2. **Explicit Over Implicit**: All state mutations are visible
3. **Separation of Concerns**: State mutations vs. external side effects
4. **Crash Recovery**: System can always resume from persisted state
5. **Testability**: Simulation testing finds bugs humans miss
6. **Flexibility**: State can be in-memory, database transaction, or hybrid

## Common Patterns

### State Machine Skeleton (v0.3)

```rust
use fasm::{Input, StateMachine, actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes}};

struct MyStateMachine {
    data: HashMap<Id, Data>,
    pending: HashMap<RequestId, PendingRequest>,
    next_id: u64,
}

struct MyTracked;
impl TrackedActionTypes for MyTracked {
    type Id = u64;
    type Action = MyRequest;
    type Result = MyResult;
}

impl StateMachine for MyStateMachine {
    type State = Self;
    type Input = MyInput;
    type TrackedAction = MyTracked;
    type UntrackedAction = MyUntracked;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type TransitionError = MyError;
    type RestoreError = ();

    async fn stf<'s, 'a>(
        state: &'s mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'a mut Self::Actions,
    ) -> Result<(), Self::TransitionError> {
        match input {
            Input::Normal(request) => {
                // 1. Validate
                // 2. Fallible operations (actions.add)
                // 3. Mutate state
                Ok(())
            }
            Input::TrackedActionCompleted { id, result } => {
                // Handle tracked action completion
                Ok(())
            }
        }
    }

    async fn restore<'s, 'a>(
        state: &'s Self::State,
        actions: &'a mut Self::Actions,
    ) -> Result<(), Self::RestoreError> {
        for (&id, pending) in &state.pending {
            actions.add(Action::Tracked(TrackedAction::new(id, ...)))?;
        }
        Ok(())
    }
}
```

### Atomicity Pattern

For in-memory state, perform fallible operations before mutations:

```rust
async fn stf(state: &mut State, input: Input, actions: &mut Actions) -> Result<(), Error> {
    // 1. Validation (can return Err)
    if !state.is_valid(&input) {
        return Err(Error::Invalid);
    }

    // 2. Prepare values (no mutation yet)
    let id = state.next_id;

    // 3. Fallible operations first
    actions.add(Action::Tracked(...))?;

    // 4. Now mutate state (point of no return)
    state.next_id += 1;
    state.pending.insert(id, ...);

    Ok(())
}
```

### Invariant Checking

```rust
impl MyState {
    pub fn check_invariants(&self) -> Result<(), String> {
        // 1. Check consistency
        // 2. Check no conflicts
        // 3. Check referential integrity
        Ok(())
    }
}
```

### Simulation Testing

```rust
#[test]
async fn test_simulation() {
    let mut rng = ChaCha8Rng::seed_from_u64(12345);
    let mut state = MyStateMachine::new();
    let mut actions = Vec::new();

    for i in 0..100_000 {
        let input = generate_random_input(&mut rng);
        let _ = MyStateMachine::stf(&mut state, input, &mut actions).await;
        actions.clear();
        state.check_invariants()
            .expect(&format!("Invariant violated at iteration {}", i));
    }
}
```

## Version 0.3 Changes

- **Simplified trait**: Uses `async fn` with `impl Future + use<'state, 'actions, Self>` syntax (Rust 2024)
- **Renamed field**: `Input::TrackedActionCompleted { id, result }` (was `res`)
- **No more GATs**: No need for `type StfFuture<'state, 'actions>` or `type RestoreFuture<...>`
- **Cleaner implementations**: Just write `async fn` in your impl block

## Examples

See the examples directory:
- `csm.rs` - Simple counter state machine
- `coffee_shop.rs` - Loyalty app with tracked/untracked actions and restore

See `dentist_booking/` crate for a full example:
- Weekly schedules with multiple time ranges
- Variable appointment durations
- Auto-selection algorithm
- Payment preauthorization
- Comprehensive simulation test suite

## See Also

- [Main crate docs](../src/lib.rs) - Trait definitions with extensive docs
- [Actions module](../src/actions.rs) - Action system documentation
- [Dentist booking example](../dentist_booking/) - Full working example