# Testing Guide

PHASM enables **deterministic simulation testing** - the killer feature for building correct systems.

## Why Simulation Testing?

Traditional testing:
- Write specific test cases
- Hard to think of edge cases
- Flaky due to timing/randomness
- Can't test crash scenarios easily

Simulation testing:
- Generate thousands of random operations
- Check invariants after every single one
- Deterministic (same seed = same test)
- Trivial to test crash/restore

## Basic Pattern

```rust
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[test]
async fn test_booking_simulation() {
    let mut rng = ChaCha8Rng::seed_from_u64(12345); // Deterministic!
    let mut state = BookingSystem::new();
    let mut actions = Vec::new();
    
    for i in 0..10_000 {
        // Generate random operation
        let input = match rng.gen_range(0..3) {
            0 => Input::RequestBooking { 
                day: random_day(&mut rng),
                time: random_time(&mut rng),
            },
            1 => Input::CancelBooking { ... },
            2 => complete_random_pending(&mut rng, &state),
        };
        
        // Execute
        let _ = BookingSystem::stf(&mut state, input, &mut actions).await;
        
        // CRITICAL: Check invariants after EVERY operation
        state.check_invariants()
            .expect(&format!("Invariant violated at iteration {}", i));
        
        actions.clear();
    }
    
    println!("✓ 10,000 random operations, all invariants satisfied");
}
```

## Time-Bounded Simulations

Run as many seeds as possible within a time budget:

```rust
use std::time::{Duration, Instant};

async fn run_timed_simulation(
    base_seed: u64,
    time_budget: Duration,
    ops_per_seed: usize,
) -> TestStats {
    let start = Instant::now();
    let mut stats = TestStats::default();
    
    while start.elapsed() < time_budget {
        let seed = base_seed + stats.seeds_tested as u64;
        
        match run_single_seed(seed, ops_per_seed).await {
            Ok(seed_stats) => {
                stats.seeds_tested += 1;
                stats.total_operations += seed_stats.operations;
                stats.total_bookings += seed_stats.bookings;
            }
            Err(e) => {
                panic!("Seed {} failed: {}", seed, e);
            }
        }
    }
    
    stats
}

#[test]
async fn test_one_second_stress() {
    let stats = run_timed_simulation(
        12345,                      // base seed
        Duration::from_secs(1),     // 1 second budget
        10_000                      // 10k ops per seed
    ).await;
    
    println!("Tested {} seeds ({} operations) in 1 second", 
        stats.seeds_tested, stats.total_operations);
    
    assert!(stats.seeds_tested > 100, "Should test 100+ seeds");
}
```

## Operation Generators

Build realistic operation mixes:

```rust
fn generate_operation(
    rng: &mut ChaCha8Rng,
    state: &State,
) -> Operation {
    let op_type = rng.gen_range(0..100);
    
    if op_type < 40 && !state.pending.is_empty() {
        // 40% chance: Complete pending operation
        let idx = rng.gen_range(0..state.pending.len());
        let req_id = *state.pending.keys().nth(idx).unwrap();
        let success = rng.gen_bool(0.85); // 85% success rate
        
        Operation::CompleteRequest { req_id, success }
    } else if op_type < 70 {
        // 30% chance: Request specific slot
        Operation::RequestSlot {
            day: random_day(rng),
            time: random_time(rng),
            duration: random_duration(rng),
        }
    } else {
        // 30% chance: Auto-select booking
        Operation::RequestAuto {
            days: random_days(rng, rng.gen_range(1..=3)),
            times: random_time_ranges(rng, rng.gen_range(1..=2)),
            duration: random_duration(rng),
        }
    }
}
```

## Testing Crash Recovery

Simulate crashes at random points:

```rust
#[test]
async fn test_crash_recovery() {
    let mut rng = ChaCha8Rng::seed_from_u64(99999);
    let mut state = BookingSystem::new();
    let mut actions = Vec::new();
    
    for i in 0..1000 {
        let input = generate_random_operation(&mut rng);
        BookingSystem::stf(&mut state, input, &mut actions).await.ok();
        actions.clear();
        
        // Randomly "crash" and restore
        if rng.gen_bool(0.1) { // 10% crash rate
            println!("💥 Crash at operation {}", i);
            
            // Simulate restart: call restore
            let mut restore_actions = Vec::new();
            BookingSystem::restore(&state, &mut restore_actions).await.unwrap();
            
            println!("🔄 Restored {} pending operations", restore_actions.len());
            
            // Process restored actions
            for action in restore_actions {
                if let Action::Tracked(tracked) = action {
                    // Simulate external system completing the action
                    let result = simulate_action_completion(&tracked);
                    
                    BookingSystem::stf(
                        &mut state,
                        Input::TrackedActionCompleted { 
                            id: tracked.action_id, 
                            result, 
                        },
                        &mut actions,
                    ).await.ok();
                }
            }
            
            // Invariants must still hold after restore!
            state.check_invariants()
                .expect(&format!("Invariants violated after restore at {}", i));
        }
    }
}
```

## Race Condition Testing

Test concurrent operations completing in different orders:

```rust
#[test]
async fn test_race_conditions() {
    let mut rng = ChaCha8Rng::seed_from_u64(77777);
    let mut state = BookingSystem::new();
    
    // Multiple users request same slot
    let slot = Slot { day: Day::Monday, time: Time(9, 0) };
    let mut pending = Vec::new();
    
    for user_id in 0..10 {
        if let Ok(req_id) = request_slot(&mut state, user_id, slot).await {
            pending.push(req_id);
        }
    }
    
    println!("10 users competing for same slot, {} pending", pending.len());
    
    // Complete in random order
    while !pending.is_empty() {
        let idx = rng.gen_range(0..pending.len());
        let req_id = pending.remove(idx);
        
        complete_preauth(&mut state, req_id, true).await.ok();
        state.check_invariants().unwrap();
    }
    
    // Only 1 booking should succeed
    assert_eq!(state.bookings.len(), 1, "Only one user should get the slot");
}
```

## Property-Based Testing

Verify properties hold across all random inputs:

```rust
#[test]
async fn property_no_double_bookings() {
    let mut rng = ChaCha8Rng::seed_from_u64(11111);
    let mut state = BookingSystem::new();
    
    for _ in 0..50_000 {
        let op = generate_random_operation(&mut rng);
        BookingSystem::stf(&mut state, op, &mut actions).await.ok();
        
        // PROPERTY: No two bookings can overlap
        for (slot1, booking1) in &state.bookings {
            for (slot2, booking2) in &state.bookings {
                if slot1 != slot2 {
                    assert!(
                        !bookings_overlap(slot1, booking1, slot2, booking2),
                        "Found overlapping bookings: {:?} and {:?}",
                        slot1, slot2
                    );
                }
            }
        }
    }
}

#[test]
async fn property_confirmed_requests_have_bookings() {
    // ... similar pattern ...
    
    for (req_id, request) in &state.pending {
        if request.status == Status::Confirmed {
            assert!(
                state.bookings.contains_key(&request.slot),
                "Confirmed request {} has no booking",
                req_id
            );
        }
    }
}
```

## Fuzzing-Style Testing

Generate truly random chaos:

```rust
#[test]
async fn test_chaos() {
    let mut rng = ChaCha8Rng::seed_from_u64(66666);
    let mut state = BookingSystem::new();
    
    for _ in 0..100_000 {
        // Completely random operation
        match rng.gen_range(0..10) {
            0..=5 => {
                // Random booking request
                let _ = request_booking(&mut state, &mut rng).await;
            }
            6..=8 if !state.pending.is_empty() => {
                // Random completion
                let idx = rng.gen_range(0..state.pending.len());
                let req_id = *state.pending.keys().nth(idx).unwrap();
                let success = rng.gen_bool(0.5); // 50% fail rate
                let _ = complete_request(&mut state, req_id, success).await;
            }
            _ if !state.bookings.is_empty() => {
                // Random cancellation
                let idx = rng.gen_range(0..state.bookings.len());
                let slot = *state.bookings.keys().nth(idx).unwrap();
                let _ = cancel_booking(&mut state, slot).await;
            }
            _ => {}
        }
        
        // Must survive any chaos
        state.check_invariants().unwrap();
    }
}
```

## Regression Testing

When you find a bug, add its seed:

```rust
#[test]
async fn test_regression_seed_42873() {
    // This seed found a bug where overlapping bookings were allowed
    // when two requests completed in a specific order
    let mut rng = ChaCha8Rng::seed_from_u64(42873);
    
    // ... run simulation with this seed ...
    
    // Should not panic anymore
}
```

## Test Organization

```rust
// tests/simulation.rs
mod common {
    // Shared helpers
    pub fn generate_operation(rng: &mut ChaCha8Rng) -> Operation { ... }
    pub fn random_day(rng: &mut ChaCha8Rng) -> Day { ... }
}

#[monoio::test]
async fn test_basic_operations() {
    // Fast, always runs
}

#[monoio::test]
async fn test_short_simulation() {
    // 1 second budget - runs in CI
}

#[monoio::test]
#[ignore] // Only run with --ignored
async fn test_long_simulation() {
    // 10 second budget - run manually
}

#[monoio::test]
#[ignore]
async fn test_stress() {
    // 60 second budget - nightly testing
}
```

## Example: Full Simulation Test Suite

```rust
use std::time::Duration;

#[monoio::test]
async fn test_mixed_operations() {
    let stats = run_timed_simulation(
        12345,
        Duration::from_secs(1),
        10_000
    ).await;
    
    println!(
        "Mixed: {} seeds, {} ops, {} bookings, {} conflicts",
        stats.seeds_tested,
        stats.total_operations,
        stats.total_bookings,
        stats.total_conflicts
    );
    
    assert!(stats.seeds_tested > 40);
}

#[monoio::test]
async fn test_high_contention() {
    // Weight operations toward conflicts
    let stats = run_timed_simulation_with_config(
        67890,
        Duration::from_secs(1),
        10_000,
        SimConfig {
            same_slot_probability: 0.8, // 80% try same slots
            complete_probability: 0.2,
        }
    ).await;
    
    assert!(
        stats.total_conflicts > stats.total_bookings,
        "Should have more conflicts than bookings in high contention"
    );
}

#[monoio::test]
async fn test_payment_failures() {
    let stats = run_timed_simulation_with_config(
        11111,
        Duration::from_secs(1),
        10_000,
        SimConfig {
            payment_success_rate: 0.5, // 50% failure rate
            ..Default::default()
        }
    ).await;
    
    assert!(
        stats.payment_failures > 0,
        "Should have payment failures"
    );
}
```

## Best Practices

1. **Always use seeded RNG**: `ChaCha8Rng::seed_from_u64(seed)`
2. **Check invariants after every operation**: Catches bugs immediately
3. **Test with time budgets**: Run as many seeds as possible
4. **Mix operation types**: Don't just test one path
5. **Test edge cases**: Empty state, full schedule, all pending
6. **Test failure modes**: Payment failures, conflicts, invalid inputs
7. **Test restore**: Simulate crashes at random points
8. **Log seeds**: When test fails, you can reproduce exactly

## Debugging Failed Simulations

```rust
// Run just the failing seed
#[test]
async fn test_debug_seed_12345() {
    let mut rng = ChaCha8Rng::seed_from_u64(12345);
    let mut state = BookingSystem::new();
    
    for i in 0..10_000 {
        let op = generate_operation(&mut rng);
        
        println!("Operation {}: {:?}", i, op);
        
        let result = state.stf(op, &mut actions).await;
        println!("Result: {:?}", result);
        println!("State: {:#?}", state);
        
        state.check_invariants()
            .expect(&format!("Failed at iteration {}", i));
    }
}
```

## Real-World Results

From the dentist booking system:

```
Test: test_mixed_operations_simulation
  Time budget: 1 second
  Operations per seed: 10,000
  Result: 252 seeds tested
  Total operations: 2,520,000
  Invariant checks: 2,520,000
  Failures: 0
  
✓ All invariants satisfied across 2.52M state transitions
```

This level of testing is impractical with manual test cases but trivial with simulation testing.
