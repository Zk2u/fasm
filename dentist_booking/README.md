# Dentist Booking System

A comprehensive appointment booking system built as a **Fallible Async State Machine (FASM)** to demonstrate advanced state machine design with deterministic simulation testing.

## Features

- **Weekly Schedule Management**: Multiple time ranges per day (e.g., morning/afternoon with lunch breaks)
- **Variable Appointment Durations**: 15-60 minutes (cleaning, checkup, filling, root canal)
- **Auto-Selection**: Clients provide preferences, system finds best available slot
- **Race Condition Handling**: Multiple users competing for same slots resolved deterministically
- **Payment Preauthorization**: Tracked actions for payment with proper rollback
- **Crash Recovery**: Full restore functionality for pending operations
- **Invariant Checking**: Comprehensive validation of system state

## Architecture

This system leverages FASM's key features:

### Tracked Actions
- **Payment Preauthorization**: Hold funds, wait for confirmation
- **Payment Release**: Cancel holds when slots are taken
- **Status Checks**: Query payment processor after crash

### Untracked Actions
- User notifications
- Analytics logging
- UI updates

### State Machine Properties
- **Deterministic**: Same inputs always produce same outputs
- **Crash-Safe**: Restore pending operations from persisted state
- **Testable**: Comprehensive simulation testing with seeded RNGs

## Running Tests

### Simple Demo
```bash
cargo run --example simple
```

Shows basic booking flow with invariant checking.

### Simulation Testing
```bash
cargo test
```

Runs comprehensive test scenarios including:

**Integration Tests:**
1. **Basic Booking Flow** - Request, preauth, and confirm a booking
2. **Slot Conflict Resolution** - Multiple users competing for same slot
3. **Auto-Selection** - Smart slot finding with various constraints
4. **Invariants After Operations** - Verify state consistency across multiple bookings
5. **Booking Preferences Honored** - Extensive validation that all bookings match user preferences

**Simulation Tests:**
1. **Mixed Operations** - Random operations with deterministic RNG
2. **High Contention** - Multiple users competing for limited slots
3. **Payment Failures** - 50% payment failure rate, verify clean state
4. **Auto-Selection Heavy** - Stress test preference-based slot finding
5. **Sparse Schedule** - Test with limited availability
6. **Long Simulation** - Extended stress testing
7. **Full Stress Test** - Maximum load testing
8. **Booking Preferences Simulation** - Verify preferences honored in random scenarios

### Test Output
```bash
cargo test -- --nocapture
```

```
running 5 tests (integration)
test test_basic_booking_flow ... ok
test test_slot_conflict ... ok
test test_auto_selection ... ok
test test_invariants_after_operations ... ok
test test_booking_preferences_honored ... ok

test result: ok. 5 passed; 0 failed

running 8 tests (simulation)
Preferences test: 11 bookings verified
test test_booking_preferences_simulation ... ok
Payment failures: 2 seeds, 20000 total ops, 124 bookings, 26 payment failures
test test_payment_failure_simulation ... ok
Auto-selection heavy: 2 seeds, 20000 total ops, 126 bookings
test test_auto_selection_heavy_simulation ... ok
High contention: 2 seeds, 20000 total ops, 130 bookings, 19696 conflicts
test test_high_contention_simulation ... ok
Sparse schedule: 2 seeds, 20000 total ops, 131 bookings, 19666 conflicts
test test_sparse_schedule_simulation ... ok
Mixed operations: 2 seeds, 20000 total ops, 133 bookings, 19652 conflicts, 34 payment failures
test test_mixed_operations_simulation ... ok
Long simulation: 2 seeds, 40000 total ops, 133 bookings, 39642 conflicts, 34 payment failures
test test_long_simulation ... ok
Stress test: 1 seeds, 50000 total ops, 65 bookings, 49842 conflicts, 10 payment failures
test test_stress_simulation ... ok

test result: ok. 8 passed; 0 failed
```

## Invariants Checked

The simulation automatically verifies:

1. **No Overlapping Bookings**: No two appointments conflict on the same day
2. **Schedule Adherence**: All bookings fit within dentist's working hours
3. **State Consistency**: Confirmed requests match actual bookings
4. **Payment Tracking**: All preauths are properly tracked and resolved
5. **Preference Matching**: Bookings match user's requested slot, time, day, and appointment type
6. **Auto-Selection Accuracy**: Auto-selected slots fall within user's preferred days and time ranges

## Why Simulation Testing?

Building systems as state machines with FASM enables powerful testing:

- **Deterministic RNG**: Use seeded random number generators for reproducibility
- **Property-Based Testing**: Verify invariants after every state transition
- **Edge Case Discovery**: Random operations find bugs humans miss
- **Crash Scenarios**: Test recovery paths that are hard to trigger manually
- **Stress Testing**: Saturate system to find capacity limits and race conditions

## Benefits of the FASM Approach

1. **Correctness**: Invariant checking catches bugs immediately
2. **Reproducibility**: Seeded RNGs make bugs reproducible
3. **Coverage**: Random testing explores state space thoroughly
4. **Confidence**: 90,000+ operations tested across all scenarios
5. **Documentation**: Tests serve as executable specifications
6. **Preference Validation**: Every booking verified against original user request

## Integration

```rust
use dentist_booking::*;
use fasm::{Input, StateMachine};

let mut system = BookingSystem::with_default_schedule();
let mut actions = Vec::new();

// Request a booking
BookingSystem::stf(
    &mut system,
    Input::Normal(BookingInput::RequestSlot {
        user_id: 1,
        name: "Alice".into(),
        email: "alice@example.com".into(),
        day: Day::Monday,
        time: Time::new(9, 0),
        apt_type: AptType::Checkup,
    }),
    &mut actions,
).await?;

// Check invariants
system.check_invariants()?;
```

## Use Cases

This pattern is ideal for:

- E-commerce systems (inventory, orders, payments)
- Reservation systems (hotels, restaurants, flights)
- Workflow engines (approvals, multi-step processes)
- Distributed systems (consensus, replication)
- Financial systems (transactions, settlements)

Any system where **correctness matters** and **race conditions exist**.
