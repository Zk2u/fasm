use dentist_booking::*;
use fasm::{Input, StateMachine};

#[monoio::test]
async fn test_basic_booking_flow() {
    let mut system = BookingSystem::with_default_schedule();
    let mut actions = Vec::new();

    // Request booking
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
    )
    .await
    .expect("Failed to request slot");

    let req_id = system.next_id - 1;
    assert_eq!(system.pending.len(), 1, "Should have 1 pending request");
    actions.clear();

    // Complete preauth
    BookingSystem::stf(
        &mut system,
        Input::TrackedActionCompleted {
            id: req_id,
            result: PaymentResult::Success { amount: 75.0 },
        },
        &mut actions,
    )
    .await
    .expect("Failed to complete preauth");

    // Verify booking matches user's request
    assert_eq!(system.bookings.len(), 1, "Should have 1 confirmed booking");

    let slot = Slot {
        day: Day::Monday,
        time: Time::new(9, 0),
    };
    let booking = system
        .bookings
        .get(&slot)
        .expect("Booking should exist at requested slot");
    assert_eq!(booking.user_id, 1, "Booking should be for correct user");
    assert_eq!(booking.name, "Alice", "Booking should have correct name");
    assert_eq!(
        booking.apt_type,
        AptType::Checkup,
        "Booking should have correct appointment type"
    );

    assert!(
        system.check_invariants().is_ok(),
        "Invariants should be satisfied"
    );
}

#[monoio::test]
async fn test_slot_conflict() {
    let mut system = BookingSystem::with_default_schedule();
    let mut actions = Vec::new();

    // Book slot for Alice
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
    )
    .await
    .expect("Alice's request should succeed");

    let alice_req = system.next_id - 1;
    actions.clear();

    // Confirm Alice's booking
    BookingSystem::stf(
        &mut system,
        Input::TrackedActionCompleted {
            id: alice_req,
            result: PaymentResult::Success { amount: 75.0 },
        },
        &mut actions,
    )
    .await
    .expect("Alice's confirmation should succeed");

    actions.clear();

    // Bob tries to book same slot
    let result = BookingSystem::stf(
        &mut system,
        Input::Normal(BookingInput::RequestSlot {
            user_id: 2,
            name: "Bob".into(),
            email: "bob@example.com".into(),
            day: Day::Monday,
            time: Time::new(9, 0),
            apt_type: AptType::Checkup,
        }),
        &mut actions,
    )
    .await;

    assert!(result.is_err(), "Bob's request should fail - slot taken");
    assert_eq!(system.bookings.len(), 1, "Should still have only 1 booking");
}

#[monoio::test]
async fn test_auto_selection() {
    let mut system = BookingSystem::with_default_schedule();
    let mut actions = Vec::new();

    // Request auto-selection
    let result = BookingSystem::stf(
        &mut system,
        Input::Normal(BookingInput::RequestAuto {
            user_id: 1,
            name: "Alice".into(),
            email: "alice@example.com".into(),
            days: vec![Day::Monday, Day::Tuesday],
            times: vec![TimeRange::new(Time::new(9, 0), Time::new(12, 0))],
            apt_type: AptType::Checkup,
        }),
        &mut actions,
    )
    .await;

    assert!(result.is_ok(), "Auto-selection should find a slot");
    assert_eq!(system.pending.len(), 1, "Should have 1 pending request");

    // Verify slot was selected and matches user preferences
    let pending = system.pending.values().next().unwrap();
    assert!(pending.slot.is_some(), "Should have selected a slot");

    let slot = pending.slot.unwrap();
    assert!(
        system.is_available(slot, AptType::Checkup.dur()),
        "Selected slot should be available"
    );

    // Verify the selected slot matches user preferences
    let requested_days = vec![Day::Monday, Day::Tuesday];
    assert!(
        requested_days.contains(&slot.day),
        "Selected day {:?} should be in requested days {:?}",
        slot.day,
        requested_days
    );

    let requested_time_range = TimeRange::new(Time::new(9, 0), Time::new(12, 0));
    assert!(
        requested_time_range.contains(slot.time),
        "Selected time {} should be within requested range {}",
        slot.time,
        requested_time_range
    );

    assert_eq!(
        pending.apt_type,
        AptType::Checkup,
        "Appointment type should match request"
    );

    // Complete the booking and verify final state
    let req_id = system.next_id - 1;
    actions.clear();

    BookingSystem::stf(
        &mut system,
        Input::TrackedActionCompleted {
            id: req_id,
            result: PaymentResult::Success { amount: 75.0 },
        },
        &mut actions,
    )
    .await
    .expect("Preauth completion should succeed");

    // Verify the confirmed booking still matches preferences
    let confirmed_booking = system
        .bookings
        .get(&slot)
        .expect("Booking should be confirmed");
    assert_eq!(
        confirmed_booking.user_id, 1,
        "Confirmed booking should be for correct user"
    );
    assert_eq!(
        confirmed_booking.apt_type,
        AptType::Checkup,
        "Confirmed booking should have correct type"
    );
}

#[monoio::test]
async fn test_invariants_after_operations() {
    let mut system = BookingSystem::with_default_schedule();
    let mut actions = Vec::new();

    // Book multiple appointments
    for i in 0..5 {
        let result = BookingSystem::stf(
            &mut system,
            Input::Normal(BookingInput::RequestSlot {
                user_id: i + 1,
                name: format!("User{}", i + 1),
                email: format!("user{}@example.com", i + 1),
                day: Day::Monday,
                time: Time::new(9, 0).add((i * 30) as u16),
                apt_type: AptType::Checkup,
            }),
            &mut actions,
        )
        .await;

        if result.is_ok() {
            let req_id = system.next_id - 1;
            actions.clear();

            BookingSystem::stf(
                &mut system,
                Input::TrackedActionCompleted {
                    id: req_id,
                    result: PaymentResult::Success { amount: 75.0 },
                },
                &mut actions,
            )
            .await
            .expect("Preauth should succeed");

            actions.clear();

            // Verify the booking matches what was requested
            let expected_slot = Slot {
                day: Day::Monday,
                time: Time::new(9, 0).add((i * 30) as u16),
            };
            if let Some(booking) = system.bookings.get(&expected_slot) {
                assert_eq!(booking.user_id, i + 1, "Booking should be for correct user");
                assert_eq!(
                    booking.apt_type,
                    AptType::Checkup,
                    "Booking should have correct appointment type"
                );
            }
        }

        // Check invariants after each operation
        system
            .check_invariants()
            .expect("Invariants should hold after each operation");
    }

    assert!(system.bookings.len() > 0, "Should have some bookings");

    // Verify all bookings match their original requests
    for (slot, booking) in &system.bookings {
        assert_eq!(
            booking.apt_type,
            AptType::Checkup,
            "All bookings should be for Checkup appointments"
        );
        assert_eq!(
            slot.day,
            Day::Monday,
            "All bookings should be on Monday as requested"
        );
    }
}

#[monoio::test]
async fn test_booking_preferences_honored() {
    let mut system = BookingSystem::with_default_schedule();
    let mut actions = Vec::new();

    // Test 1: Specific slot request - verify exact match
    BookingSystem::stf(
        &mut system,
        Input::Normal(BookingInput::RequestSlot {
            user_id: 1,
            name: "Alice".into(),
            email: "alice@example.com".into(),
            day: Day::Wednesday,
            time: Time::new(14, 30),
            apt_type: AptType::Filling,
        }),
        &mut actions,
    )
    .await
    .expect("Slot request should succeed");

    let req_id_1 = system.next_id - 1;
    let pending_1 = system.pending.get(&req_id_1).unwrap();

    assert_eq!(
        pending_1.slot,
        Some(Slot {
            day: Day::Wednesday,
            time: Time::new(14, 30),
        }),
        "Requested slot should match exactly"
    );
    assert_eq!(
        pending_1.apt_type,
        AptType::Filling,
        "Appointment type should match request"
    );
    assert_eq!(pending_1.user_id, 1, "User ID should match");

    actions.clear();

    // Confirm booking
    BookingSystem::stf(
        &mut system,
        Input::TrackedActionCompleted {
            id: req_id_1,
            result: PaymentResult::Success {
                amount: AptType::Filling.price(),
            },
        },
        &mut actions,
    )
    .await
    .expect("Confirmation should succeed");

    let slot_1 = Slot {
        day: Day::Wednesday,
        time: Time::new(14, 30),
    };
    let booking_1 = system.bookings.get(&slot_1).expect("Booking should exist");
    assert_eq!(booking_1.user_id, 1, "Confirmed booking user should match");
    assert_eq!(
        booking_1.apt_type,
        AptType::Filling,
        "Confirmed booking type should match"
    );
    assert_eq!(booking_1.name, "Alice", "Name should be preserved");
    assert_eq!(
        booking_1.email, "alice@example.com",
        "Email should be preserved"
    );

    actions.clear();

    // Test 2: Auto-selection - verify slot is within preferences
    BookingSystem::stf(
        &mut system,
        Input::Normal(BookingInput::RequestAuto {
            user_id: 2,
            name: "Bob".into(),
            email: "bob@example.com".into(),
            days: vec![Day::Tuesday, Day::Thursday],
            times: vec![
                TimeRange::new(Time::new(10, 0), Time::new(13, 0)),
                TimeRange::new(Time::new(14, 0), Time::new(16, 0)),
            ],
            apt_type: AptType::RootCanal,
        }),
        &mut actions,
    )
    .await
    .expect("Auto-selection should succeed");

    let req_id_2 = system.next_id - 1;
    let pending_2 = system.pending.get(&req_id_2).unwrap();
    let selected_slot = pending_2.slot.expect("Auto-selection should find a slot");

    // Verify day preference
    assert!(
        vec![Day::Tuesday, Day::Thursday].contains(&selected_slot.day),
        "Selected day {:?} should be in preferred days [Tuesday, Thursday]",
        selected_slot.day
    );

    // Verify time preference
    let time_ranges = vec![
        TimeRange::new(Time::new(10, 0), Time::new(13, 0)),
        TimeRange::new(Time::new(14, 0), Time::new(16, 0)),
    ];
    let time_matches = time_ranges
        .iter()
        .any(|range| range.contains(selected_slot.time));
    assert!(
        time_matches,
        "Selected time {} should be within one of the preferred time ranges",
        selected_slot.time
    );

    // Verify appointment type and duration
    assert_eq!(
        pending_2.apt_type,
        AptType::RootCanal,
        "Appointment type should match"
    );
    assert!(
        system.is_available(selected_slot, AptType::RootCanal.dur()),
        "Selected slot should fit the 60-minute root canal appointment"
    );

    actions.clear();

    // Confirm the auto-selected booking
    BookingSystem::stf(
        &mut system,
        Input::TrackedActionCompleted {
            id: req_id_2,
            result: PaymentResult::Success {
                amount: AptType::RootCanal.price(),
            },
        },
        &mut actions,
    )
    .await
    .expect("Auto-selected booking confirmation should succeed");

    let booking_2 = system
        .bookings
        .get(&selected_slot)
        .expect("Auto-selected booking should be confirmed");
    assert_eq!(
        booking_2.user_id, 2,
        "Auto-selected booking should be for correct user"
    );
    assert_eq!(
        booking_2.apt_type,
        AptType::RootCanal,
        "Auto-selected booking should preserve appointment type"
    );

    // Test 3: Different appointment durations work correctly
    for (user_id, apt_type) in [(3, AptType::Cleaning), (4, AptType::Checkup)] {
        actions.clear();

        BookingSystem::stf(
            &mut system,
            Input::Normal(BookingInput::RequestSlot {
                user_id,
                name: format!("User{}", user_id),
                email: format!("user{}@example.com", user_id),
                day: Day::Friday,
                time: Time::new(9, 0).add(((user_id - 3) * 60) as u16),
                apt_type,
            }),
            &mut actions,
        )
        .await
        .expect("Different appointment types should be bookable");

        let req_id = system.next_id - 1;
        let pending = system.pending.get(&req_id).unwrap();
        assert_eq!(
            pending.apt_type, apt_type,
            "Appointment type should be preserved"
        );

        actions.clear();

        BookingSystem::stf(
            &mut system,
            Input::TrackedActionCompleted {
                id: req_id,
                result: PaymentResult::Success {
                    amount: apt_type.price(),
                },
            },
            &mut actions,
        )
        .await
        .expect("Confirmation should succeed");
    }

    // Final verification: all bookings match their types
    assert_eq!(system.bookings.len(), 4, "Should have 4 confirmed bookings");

    for (slot, booking) in &system.bookings {
        // Verify the booking occupies the slot it claims
        assert!(
            system.schedule.contains_key(&slot.day),
            "Booking should be on a scheduled day"
        );

        // Verify no overlaps (this is also checked by invariants, but let's be explicit)
        for (other_slot, other_booking) in &system.bookings {
            if slot != other_slot && slot.day == other_slot.day {
                let booking_end = slot.time.add(booking.apt_type.dur());
                let other_end = other_slot.time.add(other_booking.apt_type.dur());

                let no_overlap = booking_end <= other_slot.time || slot.time >= other_end;
                assert!(
                    no_overlap,
                    "Bookings should not overlap: {:?} ({}min) and {:?} ({}min)",
                    slot,
                    booking.apt_type.dur(),
                    other_slot,
                    other_booking.apt_type.dur()
                );
            }
        }
    }

    system
        .check_invariants()
        .expect("All invariants should be satisfied");
}
