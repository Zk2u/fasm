# Performance Guide

PHASM state machines can be highly performant when designed correctly. This guide covers optimization strategies.

## State Storage Performance

### Use Appropriate Data Structures

```rust
use ahash::HashMap; // Faster than std::HashMap
use std::collections::BTreeMap; // For ordered iteration

// ✅ Fast lookups with ahash
struct State {
    bookings: HashMap<SlotId, Booking>, // O(1) lookup
    pending: HashMap<RequestId, Request>,
}

// ✅ Ordered iteration with BTreeMap
struct State {
    bookings: BTreeMap<SlotId, Booking>, // Deterministic iteration
}

// ❌ Linear search for everything
struct State {
    bookings: Vec<Booking>, // O(n) to find booking
}
```

### Avoid Expensive Clones

```rust
// ❌ Unnecessary clones
fn check_booking(state: &State, id: &BookingId) -> Result<()> {
    let bookings = state.bookings.clone(); // Expensive!
    if bookings.contains_key(id) {
        Ok(())
    } else {
        Err(NotFound)
    }
}

// ✅ Use references
fn check_booking(state: &State, id: &BookingId) -> Result<()> {
    if state.bookings.contains_key(id) {
        Ok(())
    } else {
        Err(NotFound)
    }
}
```

### Preallocate Capacity

```rust
// ✅ Reserve capacity upfront
impl BookingSystem {
    fn new_with_capacity(capacity: usize) -> Self {
        Self {
            bookings: HashMap::with_capacity(capacity),
            pending: HashMap::with_capacity(capacity / 2),
        }
    }
}
```

## Invariant Checking Performance

### Check Invariants Strategically

```rust
// Development: Check after every operation
#[cfg(debug_assertions)]
fn maybe_check_invariants(state: &State) {
    state.check_invariants().expect("Invariant violated");
}

// Production: Check periodically or never
#[cfg(not(debug_assertions))]
fn maybe_check_invariants(state: &State) {
    // No-op in release builds
}
```

### Optimize Invariant Checks

```rust
// ❌ O(n²) overlap check every time
fn check_invariants(&self) -> Result<(), String> {
    for booking1 in &self.bookings {
        for booking2 in &self.bookings {
            if bookings_overlap(booking1, booking2) {
                return Err("Overlap found");
            }
        }
    }
    Ok(())
}

// ✅ Only check new booking against existing
fn check_can_add(&self, new_slot: Slot, duration: u16) -> Result<(), String> {
    for (existing_slot, existing_booking) in &self.bookings {
        if would_overlap(new_slot, duration, existing_slot, existing_booking) {
            return Err("Would overlap");
        }
    }
    Ok(())
}
```

### Use Incremental Validation

```rust
// ✅ Validate before modifying
fn add_booking(&mut self, slot: Slot, booking: Booking) -> Result<()> {
    // Fast check before expensive state update
    if !self.is_valid_slot(&slot) {
        return Err(InvalidSlot);
    }
    
    if self.bookings.contains_key(&slot) {
        return Err(SlotTaken);
    }
    
    // Only check new booking, not entire state
    self.check_can_add(slot, booking.duration)?;
    
    // Now safe to mutate
    self.bookings.insert(slot, booking);
    Ok(())
}
```

## Action Container Performance

### Reuse Allocations

```rust
// ✅ Reuse action vector across calls
let mut actions = Vec::with_capacity(100);

loop {
    actions.clear(); // Reuse allocation
    state_machine.stf(&mut state, input, &mut actions).await?;
    execute_actions(&actions).await?;
}
```

### Batch Actions

```rust
// ❌ Individual action for each notification
for user in &users {
    actions.add(Action::Untracked(NotifyUser { user: user.id }))?;
}

// ✅ Batch notification
actions.add(Action::Untracked(NotifyUsers { 
    user_ids: users.iter().map(|u| u.id).collect() 
}))?;
```

## Async Performance

### Avoid Unnecessary Awaits

```rust
// ❌ Awaiting when no async work needed
async fn simple_validation(state: &State, input: Input) -> Result<()> {
    if !state.is_valid(&input) {
        return Err(Invalid); // No async needed
    }
    Ok(())
}

// ✅ Use sync function when possible (or keep async for trait requirement)
fn simple_validation(state: &State, input: Input) -> Result<()> {
    if !state.is_valid(&input) {
        return Err(Invalid);
    }
    Ok(())
}
```

## Memory Efficiency

### Use Compact Representations

```rust
// ❌ Large enum with lots of variants
#[derive(Clone)]
enum Action {
    NotifyUser { user_id: u64, message: String, ... }, // 40+ bytes
    LogEvent { event: String, details: String, ... },
    // ...
}

// ✅ Box large variants
enum Action {
    NotifyUser(Box<NotifyUserData>), // 8 bytes
    LogEvent(Box<LogEventData>),
}
```

### Avoid String Allocations

```rust
// ❌ String allocations for error messages
fn validate(&self) -> Result<(), String> {
    if !self.is_valid() {
        return Err(format!("Invalid state: {:?}", self)); // Allocation!
    }
    Ok(())
}

// ✅ Static error types
#[derive(Debug)]
enum ValidationError {
    InvalidState,
    ConflictingBooking { slot: Slot },
}

fn validate(&self) -> Result<(), ValidationError> {
    if !self.is_valid() {
        return Err(ValidationError::InvalidState); // No allocation
    }
    Ok(())
}
```

## Database-Backed State

### Batch Reads and Writes

```rust
// ❌ Individual queries
async fn load_state(db: &Database) -> State {
    let bookings = db.query("SELECT * FROM bookings").await?;
    for booking in bookings {
        let details = db.query("SELECT * FROM booking_details WHERE id = ?", booking.id).await?;
        // N+1 queries!
    }
}

// ✅ Batch load
async fn load_state(db: &Database) -> State {
    let (bookings, details) = tokio::join!(
        db.query("SELECT * FROM bookings"),
        db.query("SELECT * FROM booking_details"),
    );
    
    // Build state from joined data
    merge_bookings_and_details(bookings, details)
}
```

### Use Transactions for Atomicity

```rust
async fn persist_state(db: &mut Database, state: &State) -> Result<()> {
    let mut txn = db.begin_transaction().await?;
    
    // All writes in one transaction
    txn.execute("UPDATE bookings SET ...").await?;
    txn.execute("INSERT INTO pending ...").await?;
    txn.execute("DELETE FROM completed ...").await?;
    
    txn.commit().await?; // Atomic!
    Ok(())
}
```

## Benchmarking

### Measure Your Hot Paths

```rust
use std::time::Instant;

#[test]
fn bench_booking_throughput() {
    let mut state = BookingSystem::new();
    let mut actions = Vec::new();
    
    let start = Instant::now();
    let count = 100_000;
    
    for i in 0..count {
        let input = generate_test_input(i);
        state.stf(input, &mut actions).await.ok();
        actions.clear();
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    
    println!("{:.0} ops/sec", ops_per_sec);
    assert!(ops_per_sec > 10_000.0, "Too slow!");
}
```

## Release Builds

Always benchmark in release mode:

```bash
cargo test --release -- --nocapture bench_
cargo build --release
```

Debug builds are 10-100x slower due to:
- No optimizations
- Bounds checking
- Debug symbols
- Invariant checks

## Real-World Performance

From our dentist booking simulation:

```
Configuration:
- ahash::HashMap for state
- Invariant checking: Every operation
- Operations: 4.27 million
- Time: 3 seconds
- Rate: 1.4 million ops/sec
```

This includes:
- Full state updates
- Action emission
- Complete invariant validation
- Deterministic RNG
- Collision detection

**Conclusion**: PHASM state machines can handle millions of operations per second with full correctness checking on modern hardware.
