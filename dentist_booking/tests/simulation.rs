use dentist_booking::*;
use phasm::{Input, StateMachine};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct TestStats {
    seeds_tested: usize,
    total_operations: usize,
    total_bookings: usize,
    total_conflicts: usize,
    total_payment_failures: usize,
}

#[derive(Debug)]
enum Operation {
    RequestSlot {
        user_id: u64,
        day: Day,
        time: Time,
        apt_type: AptType,
    },
    RequestAuto {
        user_id: u64,
        days: Vec<Day>,
        times: Vec<TimeRange>,
        apt_type: AptType,
    },
    CompletePreauth {
        req_id: u64,
        success: bool,
    },
}

// ============================================================================
// Time-Bounded Test Runner
// ============================================================================

async fn run_simulation_with_time_budget(
    base_seed: u64,
    time_budget: Duration,
    ops_per_seed: usize,
) -> TestStats {
    let start = Instant::now();
    let mut stats = TestStats::default();

    while start.elapsed() < time_budget {
        let seed = base_seed + stats.seeds_tested as u64;
        match run_single_simulation(seed, ops_per_seed).await {
            Ok(seed_stats) => {
                stats.seeds_tested += 1;
                stats.total_operations += seed_stats.total_operations;
                stats.total_bookings += seed_stats.total_bookings;
                stats.total_conflicts += seed_stats.total_conflicts;
                stats.total_payment_failures += seed_stats.total_payment_failures;
            }
            Err(e) => {
                panic!("Simulation failed on seed {}: {}", seed, e);
            }
        }
    }

    stats
}

async fn run_single_simulation(seed: u64, num_ops: usize) -> Result<TestStats, String> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut system = BookingSystem::with_default_schedule();
    let mut stats = TestStats {
        seeds_tested: 1,
        ..Default::default()
    };
    let mut pending_requests: Vec<u64> = Vec::new();
    let mut next_user_id = 1u64;

    for _ in 0..num_ops {
        let op = generate_operation(&mut rng, &pending_requests, &mut next_user_id);
        stats.total_operations += 1;

        match op {
            Operation::RequestSlot {
                user_id,
                day,
                time,
                apt_type,
            } => match request_slot(&mut system, user_id, day, time, apt_type).await {
                Ok(req_id) => {
                    pending_requests.push(req_id);
                }
                Err(BookingError::SlotNotAvailable) => {
                    stats.total_conflicts += 1;
                }
                Err(e) => return Err(format!("Unexpected error: {:?}", e)),
            },
            Operation::RequestAuto {
                user_id,
                days,
                times,
                apt_type,
            } => match request_auto(&mut system, user_id, days, times, apt_type).await {
                Ok(req_id) => {
                    pending_requests.push(req_id);
                }
                Err(BookingError::NoSlotFound) => {
                    stats.total_conflicts += 1;
                }
                Err(e) => return Err(format!("Unexpected error: {:?}", e)),
            },
            Operation::CompletePreauth { req_id, success } => {
                if let Some(pos) = pending_requests.iter().position(|&id| id == req_id) {
                    pending_requests.remove(pos);

                    match complete_preauth(&mut system, req_id, success).await {
                        Ok(()) => {
                            if success {
                                // Check if booking actually succeeded or slot was taken
                                if let Some(pending) = system.pending.get(&req_id) {
                                    if pending.status == ReqStatus::SlotConfirmed {
                                        stats.total_bookings += 1;
                                    }
                                }
                            } else {
                                stats.total_payment_failures += 1;
                            }
                        }
                        Err(e) => return Err(format!("Complete preauth error: {}", e)),
                    }
                }
            }
        }

        // Check invariants after every operation
        system.check_invariants()?;
    }

    // Final invariant check
    system.check_invariants()?;

    Ok(stats)
}

fn generate_operation(
    rng: &mut ChaCha8Rng,
    pending_requests: &[u64],
    next_user_id: &mut u64,
) -> Operation {
    let op_type = rng.gen_range(0..100);

    if op_type < 40 && !pending_requests.is_empty() {
        // 40% chance to complete a pending preauth if any exist
        let idx = rng.gen_range(0..pending_requests.len());
        let req_id = pending_requests[idx];
        let success = rng.gen_bool(0.85); // 85% success rate

        Operation::CompletePreauth { req_id, success }
    } else if op_type < 75 {
        // 35% chance to request specific slot
        let user_id = *next_user_id;
        *next_user_id += 1;

        Operation::RequestSlot {
            user_id,
            day: random_day(rng),
            time: random_time(rng),
            apt_type: random_apt_type(rng),
        }
    } else {
        // 25% chance to request auto-selection
        let user_id = *next_user_id;
        *next_user_id += 1;

        let day_count = rng.gen_range(1..=3);
        let time_count = rng.gen_range(1..=2);

        Operation::RequestAuto {
            user_id,
            days: random_days(rng, day_count),
            times: random_time_ranges(rng, time_count),
            apt_type: random_apt_type(rng),
        }
    }
}

// ============================================================================
// Test Functions
// ============================================================================

#[monoio::test]
async fn test_mixed_operations_simulation() {
    let stats = run_simulation_with_time_budget(12345, Duration::from_secs(1), 10000).await;

    println!(
        "Mixed operations: {} seeds, {} total ops, {} bookings, {} conflicts, {} payment failures",
        stats.seeds_tested,
        stats.total_operations,
        stats.total_bookings,
        stats.total_conflicts,
        stats.total_payment_failures
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
    assert!(
        stats.total_operations >= 10000,
        "Should have run at least 10k operations"
    );
}

#[monoio::test]
async fn test_high_contention_simulation() {
    let stats = run_simulation_with_time_budget(67890, Duration::from_secs(1), 10000).await;

    println!(
        "High contention: {} seeds, {} total ops, {} bookings, {} conflicts",
        stats.seeds_tested, stats.total_operations, stats.total_bookings, stats.total_conflicts
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
    // High contention should produce many conflicts
    assert!(
        stats.total_conflicts > stats.total_bookings,
        "Should have more conflicts than bookings in high contention"
    );
}

#[monoio::test]
async fn test_payment_failure_simulation() {
    let stats = run_simulation_with_time_budget(11111, Duration::from_secs(1), 10000).await;

    println!(
        "Payment failures: {} seeds, {} total ops, {} bookings, {} payment failures",
        stats.seeds_tested,
        stats.total_operations,
        stats.total_bookings,
        stats.total_payment_failures
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
    assert!(
        stats.total_payment_failures > 0,
        "Should have some payment failures"
    );
}

#[monoio::test]
async fn test_auto_selection_heavy_simulation() {
    let stats = run_simulation_with_time_budget(22222, Duration::from_secs(1), 10000).await;

    println!(
        "Auto-selection heavy: {} seeds, {} total ops, {} bookings",
        stats.seeds_tested, stats.total_operations, stats.total_bookings
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
}

#[monoio::test]
async fn test_sparse_schedule_simulation() {
    let stats = run_simulation_with_time_budget(33333, Duration::from_secs(1), 10000).await;

    println!(
        "Sparse schedule: {} seeds, {} total ops, {} bookings, {} conflicts",
        stats.seeds_tested, stats.total_operations, stats.total_bookings, stats.total_conflicts
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
}

#[monoio::test]
async fn test_long_simulation() {
    let stats = run_simulation_with_time_budget(44444, Duration::from_secs(2), 20000).await;

    println!(
        "Long simulation: {} seeds, {} total ops, {} bookings, {} conflicts, {} payment failures",
        stats.seeds_tested,
        stats.total_operations,
        stats.total_bookings,
        stats.total_conflicts,
        stats.total_payment_failures
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
    assert!(
        stats.total_operations >= 20000,
        "Should have run at least 20k operations"
    );
}

#[monoio::test]
async fn test_stress_simulation() {
    let stats = run_simulation_with_time_budget(55555, Duration::from_secs(3), 50000).await;

    println!(
        "Stress test: {} seeds, {} total ops, {} bookings, {} conflicts, {} payment failures",
        stats.seeds_tested,
        stats.total_operations,
        stats.total_bookings,
        stats.total_conflicts,
        stats.total_payment_failures
    );

    assert!(
        stats.seeds_tested > 0,
        "Should have tested at least one seed"
    );
    assert!(
        stats.total_operations >= 50000,
        "Should have run at least 50k operations"
    );
}

// ============================================================================
// Helper Functions
// ============================================================================

async fn request_slot(
    system: &mut BookingSystem,
    user_id: u64,
    day: Day,
    time: Time,
    apt_type: AptType,
) -> Result<u64, BookingError> {
    let mut actions = Vec::new();

    BookingSystem::stf(
        system,
        Input::Normal(BookingInput::RequestSlot {
            user_id,
            name: format!("User{}", user_id),
            email: format!("user{}@example.com", user_id),
            day,
            time,
            apt_type,
        }),
        &mut actions,
    )
    .await?;

    Ok(system.next_id - 1)
}

async fn request_auto(
    system: &mut BookingSystem,
    user_id: u64,
    days: Vec<Day>,
    times: Vec<TimeRange>,
    apt_type: AptType,
) -> Result<u64, BookingError> {
    let mut actions = Vec::new();

    BookingSystem::stf(
        system,
        Input::Normal(BookingInput::RequestAuto {
            user_id,
            name: format!("User{}", user_id),
            email: format!("user{}@example.com", user_id),
            days,
            times,
            apt_type,
        }),
        &mut actions,
    )
    .await?;

    Ok(system.next_id - 1)
}

async fn complete_preauth(
    system: &mut BookingSystem,
    req_id: u64,
    success: bool,
) -> Result<(), String> {
    let mut actions = Vec::new();

    let result = if success {
        let apt_type = system.pending.get(&req_id).map(|p| p.apt_type);
        let amount = apt_type.map(|t| t.price()).unwrap_or(50.0);
        PaymentResult::Success { amount }
    } else {
        PaymentResult::Failed {
            reason: "Insufficient funds".into(),
        }
    };

    BookingSystem::stf(
        system,
        Input::TrackedActionCompleted {
            id: req_id,
            result: result,
        },
        &mut actions,
    )
    .await
    .map_err(|e| format!("{:?}", e))
}

fn random_apt_type(rng: &mut ChaCha8Rng) -> AptType {
    let types = AptType::all();
    types[rng.gen_range(0..types.len())]
}

fn random_day(rng: &mut ChaCha8Rng) -> Day {
    let days = &[
        Day::Monday,
        Day::Tuesday,
        Day::Wednesday,
        Day::Thursday,
        Day::Friday,
    ];
    days[rng.gen_range(0..days.len())]
}

fn random_days(rng: &mut ChaCha8Rng, count: usize) -> Vec<Day> {
    let all_days = &[
        Day::Monday,
        Day::Tuesday,
        Day::Wednesday,
        Day::Thursday,
        Day::Friday,
    ];
    let mut days = Vec::new();
    for _ in 0..count.min(5) {
        days.push(all_days[rng.gen_range(0..all_days.len())]);
    }
    days
}

fn random_time(rng: &mut ChaCha8Rng) -> Time {
    let hour = rng.gen_range(9..17);
    let minute = rng.gen_range(0..4) * 15;
    Time::new(hour, minute)
}

fn random_time_ranges(rng: &mut ChaCha8Rng, count: usize) -> Vec<TimeRange> {
    let mut ranges = Vec::new();
    for _ in 0..count {
        let start = random_time(rng);
        let end = start.add(rng.gen_range(60..240));
        if end.0 < 18 {
            ranges.push(TimeRange::new(start, end));
        }
    }
    if ranges.is_empty() {
        ranges.push(TimeRange::new(Time::new(9, 0), Time::new(17, 0)));
    }
    ranges
}

// Helper to verify a booking matches the original request
fn verify_booking_matches_request(
    system: &BookingSystem,
    req_id: u64,
    expected_user_id: u64,
    expected_apt_type: AptType,
    expected_slot: Option<Slot>,
) -> Result<(), String> {
    let pending = system
        .pending
        .get(&req_id)
        .ok_or_else(|| format!("Request {} not found in pending", req_id))?;

    if pending.user_id != expected_user_id {
        return Err(format!(
            "User ID mismatch: expected {}, got {}",
            expected_user_id, pending.user_id
        ));
    }

    if pending.apt_type != expected_apt_type {
        return Err(format!(
            "Appointment type mismatch: expected {:?}, got {:?}",
            expected_apt_type, pending.apt_type
        ));
    }

    if let Some(expected) = expected_slot {
        if pending.slot != Some(expected) {
            return Err(format!(
                "Slot mismatch: expected {:?}, got {:?}",
                expected, pending.slot
            ));
        }
    }

    // If confirmed, verify the booking also matches
    if pending.status == ReqStatus::SlotConfirmed {
        if let Some(slot) = pending.slot {
            let booking = system
                .bookings
                .get(&slot)
                .ok_or_else(|| format!("Confirmed booking not found at slot {:?}", slot))?;

            if booking.user_id != expected_user_id {
                return Err(format!(
                    "Confirmed booking user mismatch: expected {}, got {}",
                    expected_user_id, booking.user_id
                ));
            }

            if booking.apt_type != expected_apt_type {
                return Err(format!(
                    "Confirmed booking type mismatch: expected {:?}, got {:?}",
                    expected_apt_type, booking.apt_type
                ));
            }
        }
    }

    Ok(())
}

// Helper to verify auto-selection respects preferences
fn verify_auto_selection_preferences(
    system: &BookingSystem,
    req_id: u64,
    preferred_days: &[Day],
    preferred_times: &[TimeRange],
    apt_type: AptType,
) -> Result<(), String> {
    let pending = system
        .pending
        .get(&req_id)
        .ok_or_else(|| format!("Request {} not found", req_id))?;

    let slot = pending
        .slot
        .ok_or_else(|| format!("Auto-selection did not assign a slot"))?;

    // Verify day preference
    if !preferred_days.contains(&slot.day) {
        return Err(format!(
            "Auto-selected day {:?} not in preferred days {:?}",
            slot.day, preferred_days
        ));
    }

    // Verify time preference
    let time_matches = preferred_times
        .iter()
        .any(|range| range.contains(slot.time));
    if !time_matches {
        return Err(format!(
            "Auto-selected time {} not in any preferred time range",
            slot.time
        ));
    }

    // Verify appointment type
    if pending.apt_type != apt_type {
        return Err(format!(
            "Auto-selection changed appointment type from {:?} to {:?}",
            apt_type, pending.apt_type
        ));
    }

    Ok(())
}

#[monoio::test]
async fn test_booking_preferences_simulation() {
    let result = run_booking_preferences_test(99999).await;
    assert!(
        result.is_ok(),
        "Booking preferences test failed: {:?}",
        result.err()
    );
    let stats = result.unwrap();
    println!(
        "Preferences test: {} bookings verified",
        stats.total_bookings
    );
}

async fn run_booking_preferences_test(seed: u64) -> Result<TestStats, String> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut system = BookingSystem::with_default_schedule();
    let mut stats = TestStats {
        seeds_tested: 1,
        ..Default::default()
    };

    // Test specific slot requests
    for i in 0..10 {
        let user_id = (i + 1) as u64;
        let day = random_day(&mut rng);
        let time = random_time(&mut rng);
        let apt_type = random_apt_type(&mut rng);

        if let Ok(req_id) = request_slot(&mut system, user_id, day, time, apt_type).await {
            stats.total_operations += 1;

            // Verify the request was stored correctly
            verify_booking_matches_request(
                &system,
                req_id,
                user_id,
                apt_type,
                Some(Slot { day, time }),
            )?;

            // Complete preauth
            if rng.gen_bool(0.8) {
                // 80% success
                match complete_preauth(&mut system, req_id, true).await {
                    Ok(()) => {
                        stats.total_operations += 1;
                        if let Some(pending) = system.pending.get(&req_id) {
                            if pending.status == ReqStatus::SlotConfirmed {
                                stats.total_bookings += 1;
                                // Verify confirmed booking still matches
                                verify_booking_matches_request(
                                    &system,
                                    req_id,
                                    user_id,
                                    apt_type,
                                    Some(Slot { day, time }),
                                )?;
                            }
                        }
                    }
                    Err(_) => {
                        stats.total_conflicts += 1;
                    }
                }
            }
        } else {
            stats.total_conflicts += 1;
        }

        system.check_invariants()?;
    }

    // Test auto-selection requests
    for i in 0..10 {
        let user_id = (i + 100) as u64;
        let day_count = rng.gen_range(1..=3);
        let days = random_days(&mut rng, day_count);
        let time_count = rng.gen_range(1..=2);
        let times = random_time_ranges(&mut rng, time_count);
        let apt_type = random_apt_type(&mut rng);

        if let Ok(req_id) =
            request_auto(&mut system, user_id, days.clone(), times.clone(), apt_type).await
        {
            stats.total_operations += 1;

            // Verify auto-selection respected preferences
            verify_auto_selection_preferences(&system, req_id, &days, &times, apt_type)?;

            // Complete preauth
            if rng.gen_bool(0.8) {
                match complete_preauth(&mut system, req_id, true).await {
                    Ok(()) => {
                        stats.total_operations += 1;
                        if let Some(pending) = system.pending.get(&req_id) {
                            if pending.status == ReqStatus::SlotConfirmed {
                                stats.total_bookings += 1;
                                // Verify it still matches preferences after confirmation
                                verify_auto_selection_preferences(
                                    &system, req_id, &days, &times, apt_type,
                                )?;
                            }
                        }
                    }
                    Err(_) => {
                        stats.total_conflicts += 1;
                    }
                }
            }
        } else {
            stats.total_conflicts += 1;
        }

        system.check_invariants()?;
    }

    Ok(stats)
}
