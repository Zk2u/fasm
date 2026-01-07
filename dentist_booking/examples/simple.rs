use dentist_booking::*;
use phasm::{Input, StateMachine};

#[monoio::main]
async fn main() {
    println!("=== Simple Dentist Booking Demo ===\n");

    let mut system = BookingSystem::with_default_schedule();
    let mut actions = Vec::new();

    // Book Alice
    println!("Booking Alice for Monday 9:00 AM checkup...");
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
    .unwrap();
    
    let req_id = system.pending.keys().next().copied().unwrap();
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
    .unwrap();
    
    println!("✓ Alice booked\n");
    actions.clear();

    // Show final bookings
    println!("Final bookings:");
    for (slot, booking) in &system.bookings {
        println!("  {} - {} ({})", slot, booking.name, booking.apt_type.name());
    }

    // Check invariants
    match system.check_invariants() {
        Ok(()) => println!("\n✓ All invariants satisfied"),
        Err(e) => println!("\n✗ Invariant violation: {}", e),
    }
}
