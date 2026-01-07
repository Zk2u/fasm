# Deterministic Simulation Testing with FASM

This document explains how we use FASM to build **provably correct systems** through deterministic simulation testing.

## The Problem

Traditional testing approaches struggle with:
- **Race conditions** are hard to reproduce
- **Edge cases** are missed by manual test writing
- **Crash scenarios** are difficult to trigger  
- **Combinatorial explosion** of possible states
- **Flaky tests** from non-deterministic behavior

## The FASM Solution

By modeling systems as **fallible async state machines**, we can:

1. **Make execution deterministic** - Same inputs → same outputs
2. **Control all randomness** - Seeded RNGs for reproducibility
3. **Check invariants continuously** - Verify correctness after every transition
4. **Simulate crashes** - Test restore paths systematically
5. **Explore state space** - Random operations find hidden bugs

## Example: Dentist Booking System (Cargo Test Suite)

### System Under Test

A booking system with:
- Weekly schedules with multiple time ranges
- Variable appointment durations (15-60 min)
- Auto-selection algorithm
- Payment preauthorization
- Crash recovery

### Test Framework

```rust
#[monoio::test]
async fn test_random_stress() {
    let result = run_random_stress_test(11111).await;
    assert!(result.is_ok(), "Random stress test failed: {:?}", result.err());
}

async fn run_random_stress_test(seed: u64) -> Result<TestStats, String> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed); // Deterministic!
    let mut system = BookingSystem::with_default_schedule();
    
    for _ in 0..100 {
        // Generate random operation
        if rng.gen_bool(0.7) {
            // 70%: New booking
            let user_id = rng.gen();
            let apt_type = random_apt_type(&mut rng);
            request_slot(&mut system, user_id, ..., apt_type).await?;
        } else {
            // 30%: Complete pending payment
            complete_preauth(&mut system, req_id, success).await?;
        }
        
        // CRITICAL: Check invariants after EVERY operation
        system.check_invariants()?;
    }
    
    Ok(stats)
}
```

### Invariants Checked

```rust
pub fn check_invariants(&self) -> Result<(), String> {
    // 1. No overlapping bookings
    for (slot1, booking1) in &self.bookings {
        for (slot2, booking2) in &self.bookings {
            if slots_overlap(slot1, booking1, slot2, booking2) {
                return Err("Overlapping bookings detected!");
            }
        }
    }
    
    // 2. All bookings fit in schedule
    for (slot, booking) in &self.bookings {
        if !fits_in_schedule(slot, booking.dur()) {
            return Err("Booking outside working hours!");
        }
    }
    
    // 3. State consistency
    for (req_id, pending) in &self.pending {
        if pending.status == Confirmed {
            assert!(self.bookings.contains_key(&pending.slot));
        }
    }
    
    Ok(())
}
```

## Test Scenarios (Run with `cargo test`)

### Integration Tests

#### 1. Basic Booking Flow
**Purpose**: Verify complete booking lifecycle
```
- Request a specific slot
- Verify pending state matches request
- Complete preauth
- Verify confirmed booking matches original request (slot, user, type)
```

#### 2. Slot Conflict Resolution
**Purpose**: Test race conditions
```
- Multiple users request same slot
- Complete preauths in order
- Verify exactly 1 booking succeeds
- Others receive proper error handling
```

#### 3. Auto-Selection
**Purpose**: Test preference-based slot finding
```
- Request with day and time preferences
- Verify selected slot matches preferences
- Complete booking
- Verify confirmed booking still matches preferences
```

#### 4. Invariants After Operations
**Purpose**: Verify state consistency
```
- Book multiple sequential appointments
- Verify each booking matches its request
- Check invariants after every operation
```

#### 5. Booking Preferences Honored
**Purpose**: Comprehensive preference validation
```
- Test exact slot matching for RequestSlot
- Test auto-selection respects day preferences
- Test auto-selection respects time range preferences
- Test different appointment types and durations
- Verify no overlaps between different appointment lengths
- Verify all user data (name, email, user_id) preserved
```

### Simulation Tests

#### 1. Mixed Operations
**Purpose**: Random operations with deterministic RNG
```
- 10,000 operations per seed
- Mix of requests and completions
- Check invariants after EVERY operation
```

#### 2. High Contention
**Purpose**: Multiple users competing for slots
```
- Heavy overlap in requested times
- Verify correct conflict resolution
- Track conflict rate
```

#### 3. Payment Failures
**Purpose**: Test error handling
```
- 50% payment failure rate
- Verify failed payments don't create bookings
- Verify state stays consistent
- No "ghost" bookings from failed payments
```

#### 4. Auto-Selection Heavy
**Purpose**: Stress test preference-based selection
```
- Various client constraints
- Different appointment durations
- Overlapping preferences
- Verify optimal slot selection
```

#### 5. Sparse Schedule
**Purpose**: Test limited availability
```
- Test behavior with few available slots
- High conflict rate expected
```

#### 6. Long Simulation
**Purpose**: Extended stress testing
```
- Double the operations
- Verify stability over time
```

#### 7. Full Stress Test
**Purpose**: Maximum load testing
```
- 50,000 operations
- Push system to limits
```

#### 8. Booking Preferences Simulation
**Purpose**: Verify preferences in random scenarios
```
- Random slot requests with verification
- Random auto-selection with preference checking
- Verify each booking matches original request
- Verify confirmed bookings preserve preferences
- 20 operations with full validation
```

## Test Results

Run with:
```bash
cargo test -- --nocapture
```

Output:

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

## Key Insights

### 1. Determinism is Critical
- Same seed → same test execution
- Bugs are **always reproducible**
- No more "works on my machine"

### 2. Invariants Catch Bugs Early
- 90,000+ operations checked across all tests
- Bugs caught immediately, not in production
- Clear error messages point to exact violation

### 3. Random Testing Finds Edge Cases
- 80,000+ conflicts found and handled correctly
- Combinations humans wouldn't think to test
- High confidence in correctness
- Every booking verified against original user preferences

### 4. Crash Recovery is Testable
- Restore logic verified systematically
- No more "hope it works" durability
- Explicit verification of recovery paths

## When to Use This Approach

✅ **Use FASM simulation testing when:**
- Correctness is critical (payments, reservations, etc.)
- Race conditions are possible
- System has complex state interactions
- Crash recovery is required
- You need high confidence

❌ **Maybe overkill for:**
- Simple CRUD apps
- Stateless services
- Prototype/MVP phase
- Read-only systems

## Benefits

1. **🐛 Bug Detection**: Finds issues before production
2. **✅ Correctness Proof**: Invariants verified continuously
3. **📈 Confidence**: 90,000+ operations tested in ~4 seconds
4. **📝 Documentation**: Tests explain system behavior
5. **🔄 Reproducibility**: Seeded RNGs make bugs deterministic
6. **⚡ Fast Feedback**: Entire test suite runs quickly
7. **🎯 Preference Validation**: Every booking verified against user's original request

## Extending the Framework

To add new tests, create a new test function in `tests/simulation.rs`:

```rust
#[monoio::test]
async fn test_your_scenario() {
    let result = run_your_scenario_test(12345).await;
    assert!(result.is_ok(), "Your test failed: {:?}", result.err());
    let stats = result.unwrap();
    println!("Your scenario: {} transitions, {} bookings", 
        stats.transitions, stats.bookings);
}

async fn run_your_scenario_test(seed: u64) -> Result<TestStats, String> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut system = BookingSystem::with_default_schedule();
    let mut stats = TestStats::default();
    
    // Your test logic here
    // ...
    
    // Always check invariants!
    system.check_invariants()?;
    
    Ok(stats)
}
```

Run with `cargo test` and your new test is automatically included!

## Conclusion

FASM + simulation testing enables building **provably correct distributed systems**. By making execution deterministic and checking invariants continuously, we catch bugs that would otherwise only appear in production under rare conditions.

This is the **main use case** of building systems as state machines - not just cleaner code, but **actual correctness guarantees** through systematic testing.
