# Critical Invariants for Correctness

For a FASM state machine to be theoretically sound and crash-safe, these invariants MUST hold.

## Important: State Can Be External

**State is not limited to in-memory structs.** State can be:
- In-memory: `HashMap`, `Vec`, custom structs
- Database transaction: FoundationDB, PostgreSQL, SQLite
- Any storage accessed through the `state` parameter

The key rule: **All state access must go through the `state` parameter passed to STF**.

See [Database-Backed State](05_database_state.md) for detailed patterns.

## Important: State Mutations vs. External Side Effects

Throughout this document, when we say "no side effects," we mean **no external side effects**. It's crucial to understand the distinction:

### ✅ State Mutations (Allowed in STF)
- Writing to in-memory data structures
- Writing to a database **through the `state` parameter**
- Updating any storage accessed via `state`
- Incrementing counters, inserting into HashMaps, etc.

### ❌ External Side Effects (Forbidden in STF - Use Actions Instead)
- Making HTTP calls to external services
- Opening new database connections (not going through `state`)
- Writing to files (not part of `state`)
- Sending notifications directly
- Calling external APIs

**Key Principle**: If it's accessed through the `state` parameter, it's a state mutation and is allowed. If it reaches outside of `state`, it's an external side effect and must be described as an action.

Example:
```rust
// ✅ ALLOWED - database write through state
async fn stf(state: &mut DbState<'_>, ...) -> Result<()> {
    state.txn.set(b"key", b"value")?;  // This is a state mutation
    Ok(())
}

// ❌ FORBIDDEN - external HTTP call
async fn stf(state: &mut State, ...) -> Result<()> {
    http_client.post("/api").send().await?;  // External side effect!
    Ok(())
}

// ✅ CORRECT - emit action for external call
async fn stf(state: &mut State, actions: &mut Actions, ...) -> Result<()> {
    actions.add(Action::Untracked(HttpPost { url: "/api" }))?;  // Describe it
    Ok(())
}
```


**Rule**: If STF returns `Err`, state MUST be unchanged.

**Important**: This rule applies to **state only**. The `actions` container can (and often should) be modified even before returning an error.

### Why Actions Container is Different

The `actions` parameter is passed for **allocation reuse**. The caller is responsible for:
- Clearing it before/after each STF call (regardless of success or failure)
- Only executing actions if STF succeeds
- Ignoring actions if STF fails

This means you can safely emit "error feedback" actions before returning `Err`:

```rust
// ✅ PERFECTLY FINE - emit actions before error
async fn handle_request(state: &mut State, amount: u32, actions: &mut Actions) -> Result<()> {
    if state.balance < amount {
        // Emit error feedback action BEFORE returning error
        actions.add(Action::Untracked(ShowError {
            message: "Insufficient balance"
        }))?;
        return Err(InsufficientFunds);
    }
    
    // ... rest of logic
}

// Caller usage:
match stf(&mut state, input, &mut actions).await {
    Ok(()) => {
        persist_state(&state).await?;
        execute_actions(&actions).await?;  // Execute on success
    }
    Err(e) => {
        // State unchanged, actions ignored (or optionally use them for error UI)
        actions.clear();  // Clear for next iteration
        handle_error(e);
    }
}
```

### Why State Atomicity Matters

If state is partially modified when STF fails, you get:
- Corrupted state after failures
- Impossible to recover after crash
- Simulation tests become meaningless

### How to Ensure

#### Pattern 1: Validate First, Mutate Last

```rust
async fn handle_booking(state: &mut State, slot: Slot) -> Result<()> {
    // ✅ Check all preconditions BEFORE mutating
    if !state.schedule.contains(&slot.day) {
        return Err(InvalidDay);
    }
    if state.bookings.contains_key(&slot) {
        return Err(SlotTaken);
    }
    if !state.has_capacity() {
        return Err(NoCapacity);
    }
    
    // Only NOW mutate - all checks passed
    state.bookings.insert(slot, booking);
    state.capacity -= 1;
    Ok(())
}

// ❌ WRONG - mutates before validation
async fn handle_booking_wrong(state: &mut State, slot: Slot) -> Result<()> {
    state.bookings.insert(slot, booking); // Mutation!
    
    if !state.has_capacity() {
        return Err(NoCapacity); // State is now corrupted!
    }
    
    state.capacity -= 1;
    Ok(())
}
```

#### Pattern 2: Extract Data, Validate, Then Mutate

```rust
// ✅ Extract needed data before mutating
async fn complete_payment(state: &mut State, id: PaymentId, result: Result) -> Result<()> {
    // Extract immutably
    let payment = state.pending.get(&id).ok_or(NotFound)?;
    let amount = payment.amount;
    let user_id = payment.user_id;
    
    // Validate
    if result.is_failed() && !state.can_refund(amount) {
        return Err(CannotRefund);
    }
    
    // Now mutate
    let payment = state.pending.get_mut(&id).unwrap();
    payment.status = Confirmed;
    state.balance += amount;
    Ok(())
}
```

#### Pattern 3: Use Shadow Copy

```rust
// ✅ For complex updates, compute new state first
async fn apply_transaction(state: &mut State, txn: Transaction) -> Result<()> {
    // Clone relevant parts
    let mut new_accounts = state.accounts.clone();
    
    // Apply changes to shadow copy
    for transfer in &txn.transfers {
        let from_balance = new_accounts.get_mut(&transfer.from)
            .ok_or(AccountNotFound)?;
        
        if *from_balance < transfer.amount {
            return Err(InsufficientFunds); // Original state unchanged
        }
        
        *from_balance -= transfer.amount;
        *new_accounts.get_mut(&transfer.to).unwrap() += transfer.amount;
    }
    
    // Only commit if everything succeeded
    state.accounts = new_accounts;
    Ok(())
}
```

## 2. Determinism

**Rule**: `(State, Input) → (State', Actions)` must always produce the same output.

### Non-Deterministic Operations (FORBIDDEN)

```rust
// ❌ System time
let timestamp = SystemTime::now();
let timestamp = Instant::now();

// ❌ Random number generation (unseeded)
let id = Uuid::new_v4();
let random_delay = rand::random::<u64>();

// ❌ External reads
let user_data = database.query(user_id).await?;
let weather = api_client.get("/weather").await?;

// ❌ Thread IDs, memory addresses
let thread_id = std::thread::current().id();
let ptr = &state as *const _ as usize;

// ❌ Iteration order of non-deterministic collections
for (k, v) in hashmap.iter() { // HashMap iteration order is random!
    // ...
}
```

### How to Make It Deterministic

```rust
// ✅ Time comes from input
enum Input {
    UserRequest { timestamp: SystemTime, ... },
}

// ✅ Seeded RNG in state
struct State {
    rng: ChaCha8Rng, // Seeded
}

fn stf(state: &mut State, ...) {
    let random_value = state.rng.gen();
}

// ✅ External data in input
enum Input {
    ProcessUser { 
        user_id: u64,
        user_data: UserData, // Fetched by caller
    },
}

// ✅ Deterministic iteration
let mut keys: Vec<_> = hashmap.keys().collect();
keys.sort(); // Deterministic order
for key in keys {
    // ...
}
```

### Why Determinism Matters

Without determinism:
- Can't reproduce bugs from logs
- Simulation testing is meaningless
- Restore might produce different actions each time
- Can't audit state machine behavior

## 3. State Always Valid

**Rule**: After every STF (success OR failure), `state.check_invariants()` must pass.

### Define Your Invariants

```rust
impl BookingSystem {
    pub fn check_invariants(&self) -> Result<(), String> {
        // 1. No overlapping bookings
        for (slot1, booking1) in &self.bookings {
            for (slot2, booking2) in &self.bookings {
                if slot1 < slot2 && bookings_overlap(slot1, booking1, slot2, booking2) {
                    return Err(format!("Overlapping: {} and {}", slot1, slot2));
                }
            }
        }
        
        // 2. All bookings within schedule
        for (slot, booking) in &self.bookings {
            if !self.schedule.contains(slot.day) {
                return Err(format!("Booking {} outside schedule", slot));
            }
        }
        
        // 3. Pending requests match bookings
        for (id, pending) in &self.pending {
            if pending.status == Confirmed {
                if !self.bookings.contains_key(&pending.slot) {
                    return Err(format!("Confirmed {} not in bookings", id));
                }
            }
        }
        
        Ok(())
    }
}
```

### Test Invariants Continuously

```rust
#[test]
async fn test_booking_stress() {
    let mut state = BookingSystem::new();
    
    for i in 0..10000 {
        let input = generate_random_input(i);
        let result = stf(&mut state, input, &mut actions).await;
        
        // CRITICAL: Check after EVERY transition
        state.check_invariants()
            .expect(&format!("Invariant violated at iteration {}", i));
    }
}
```

## 4. Tracked Action IDs Must Be Deterministic

**Rule**: Action IDs must be derivable from state, not from randomness.

```rust
// ❌ Non-deterministic ID
fn create_request(state: &mut State, ...) {
    let id = Uuid::new_v4(); // Different on every call!
    state.pending.insert(id, request);
    emit_action(id, ...);
}

// ✅ Deterministic ID from state
fn create_request(state: &mut State, ...) {
    let id = state.next_request_id;
    state.next_request_id += 1; // State updated
    state.pending.insert(id, request);
    emit_action(id, ...);
}
```

### Why This Matters

If IDs are non-deterministic:
- Restore might generate different IDs
- Can't match completed actions to original requests
- Simulation tests produce different results each run

## 5. Tracked Actions Stored Before Emission

**Rule**: Store tracked action info in state BEFORE emitting the action.

```rust
// ✅ CORRECT ORDER
fn request_payment(state: &mut State, ...) -> Result<()> {
    let payment_id = state.next_id;
    state.next_id += 1;
    
    // 1. Store in state FIRST
    state.pending_payments.insert(payment_id, Payment {
        amount,
        user_id,
        status: Pending,
    });
    
    // 2. Then emit action
    actions.add(Action::Tracked(
        TrackedAction::new(payment_id, ChargeCard { amount })
    ))?;
    
    Ok(())
}

// ❌ WRONG ORDER
fn request_payment_wrong(state: &mut State, ...) -> Result<()> {
    let payment_id = state.next_id;
    state.next_id += 1;
    
    // 1. Emit action first
    actions.add(Action::Tracked(
        TrackedAction::new(payment_id, ChargeCard { amount })
    ))?;
    
    // 2. Store in state later
    state.pending_payments.insert(payment_id, Payment {
        amount,
        user_id,
        status: Pending,
    });
    
    // If we crash between emit and store, payment is lost!
    Ok(())
}
```

### Why This Matters

If action is emitted before storing in state:
- Crash between emit and store loses the action
- Restore can't recreate the action (not in state)
- Action completes but state doesn't know about it

## 6. Actions Are Descriptions, Not External Executions

**Rule**: STF emits action *descriptions* for external operations. Execution happens externally.

**Important**: This rule is about external side effects (HTTP calls, notifications, etc.). Writing to a database through the `state` parameter is NOT a side effect - it's a state mutation and is perfectly fine in STF.

```rust
// ❌ External side effect in STF
async fn notify_user(state: &State, user: UserId, msg: String) -> Result<()> {
    // This is an external side effect - forbidden!
    http_client.post("/notify")
        .json(&json!({ "user": user, "message": msg }))
        .send()
        .await?;
    
    Ok(())
}

// ✅ Database write through state - this is fine!
async fn update_user(state: &mut DbState<'_>, user_id: u64, name: String) -> Result<()> {
    // Writing to database through state parameter is allowed - it's a state mutation
    state.txn.set(format!("user/{}", user_id).as_bytes(), name.as_bytes())?;
    Ok(())
}

// ✅ Emit action description
async fn notify_user(state: &State, user: UserId, msg: String, actions: &mut Actions) -> Result<()> {
    // Just describe what to do
    actions.add(Action::Untracked(NotifyUser {
        user,
        message: msg,
    }))?;
    
    Ok(())
}

// Execution happens elsewhere
async fn execute_actions(actions: &[Action]) {
    for action in actions {
        match action {
            Action::Untracked(NotifyUser { user, message }) => {
                // NOW we make the HTTP call
                http_client.post("/notify")
                    .json(&json!({ "user": user, "message": message }))
                    .send()
                    .await?;
            }
            // ...
        }
    }
}
```

### Why This Matters

If STF has external side effects:
- Breaks determinism (network timing, failures)
- Can't test STF without external dependencies
- Simulation testing doesn't work
- Can't replay transitions from logs

**Note**: Database writes through the `state` parameter don't break determinism because the database transaction IS your state. It's external HTTP calls, file I/O outside of state, etc. that are forbidden.

## 7. Restore Is Pure Function of State

**Rule**: `restore()` can ONLY read from the `state` parameter.

```rust
// ❌ External dependency in restore
async fn restore(state: &State, actions: &mut Actions) -> Result<()> {
    // NO! Cannot query external systems
    let pending = database.query("SELECT * FROM pending_payments").await?;
    
    for payment in pending {
        actions.add(restore_payment_action(payment))?;
    }
    
    Ok(())
}

// ✅ Pure function of state
async fn restore(state: &State, actions: &mut Actions) -> Result<()> {
    // Only use state
    for (payment_id, payment) in &state.pending_payments {
        if payment.status == Pending {
            actions.add(Action::Tracked(
                TrackedAction::new(*payment_id, CheckPaymentStatus { payment_id })
            ))?;
        }
    }
    
    Ok(())
}
```

### Why This Matters

If restore has external dependencies:
- Can't restore if external system is down
- Restore is non-deterministic (external state changes)
- Can't test restore in isolation
- Can't audit what will be restored

## 8. Tracked Action Results Must Update State

**Rule**: Every `TrackedActionCompleted` input must update state.

```rust
// ❌ Ignoring result
match input {
    TrackedActionCompleted { id, result } => {
        log::info!("Got result for {}: {:?}", id, result);
        Ok(()) // State unchanged!
    }
}

// ✅ Update state with result
match input {
    TrackedActionCompleted { id, result } => {
        let payment = state.pending_payments.get_mut(&id)
            .ok_or(UnknownPayment)?;
        
        payment.status = match result {
            PaymentSuccess { txn_id } => {
                payment.transaction_id = Some(txn_id);
                Confirmed
            }
            PaymentFailed { reason } => {
                payment.failure_reason = Some(reason);
                Failed
            }
        };
        
        Ok(())
    }
}
```

### Why This Matters

If results don't update state:
- Restore will re-emit the same action forever
- State never reflects reality
- Can't make decisions based on action outcomes

## 9. No Shared Mutable State

**Rule**: All mutable state must be accessible through the `State` parameter.

```rust
// ❌ Hidden global state
static CACHE: Mutex<HashMap<u64, User>> = Mutex::new(HashMap::new());

async fn stf(state: &mut State, ...) {
    let mut cache = CACHE.lock().unwrap();
    cache.insert(user_id, user); // Breaks determinism!
}

// ✅ Explicit in-memory state
struct State {
    users: HashMap<u64, User>, // Part of state
}

async fn stf(state: &mut State, ...) {
    state.users.insert(user_id, user); // Deterministic
}

// ✅ Database state (also valid!)
struct DbState<'txn> {
    txn: &'txn mut Transaction, // Database IS the state
}

async fn stf(state: &mut DbState<'_>, ...) {
    state.txn.set(user_key, user_data); // Through state parameter
}
```

### Why This Matters

All state must flow through the `state` parameter so that:
- STF remains deterministic
- State can be persisted/restored
- No hidden dependencies
- Testing can control all state

**Note**: A database transaction accessed through `state` is perfectly valid. The rule is "no *external* state" - everything must be accessible through the `state` parameter.

## 10. Input Contains All External Data

**Rule**: Anything from outside (time, API responses) must be in `Input`. External *data* reads must be in `Input`, but external *state* access through `state` is allowed.

```rust
// ❌ External reads bypassing state parameter
async fn stf(state: &mut State, input: UserId) {
    let db = Database::connect("...").await?; // NO! External connection
    let user = db.query(input).await?; // NO!
    let timestamp = SystemTime::now(); // NO!
}

// ✅ External data in input (in-memory state)
enum MyInput {
    ProcessUser {
        user_id: UserId,
        user_data: UserData,    // Fetched by caller
        timestamp: SystemTime,  // Provided by caller
    }
}

async fn stf(state: &mut State, input: Input<MyInput>) {
    match input {
        Input::Normal(ProcessUser { user_id, user_data, timestamp }) => {
            // All data available, no external reads needed
            state.users.insert(user_id, user_data);
        }
    }
}

// ✅ Database accessed through state parameter (also valid!)
struct DbState<'txn> {
    txn: &'txn mut Transaction,
}

async fn stf(state: &mut DbState<'_>, input: Input<MyInput>) {
    match input {
        Input::Normal(ProcessUser { user_id, user_data, timestamp }) => {
            // Database access through state is allowed
            state.txn.set(format!("user/{}", user_id).as_bytes(), &user_data)?;
            state.txn.set(b"last_update", &timestamp.as_bytes())?;
        }
    }
}
```

### The Distinction

- ❌ **External connections**: Opening database connections, API clients in STF
- ✅ **State parameter**: Accessing database through `state.txn` or `state.db`
- ❌ **System calls**: `SystemTime::now()`, unseeded random
- ✅ **Input data**: Time, API responses passed through `Input`

If the database transaction is in `state`, reading/writing through it is perfectly valid and maintains determinism.

## The Meta-Rule: Replayability

If you can:
1. Persist state after each STF
2. Log all inputs
3. Replay inputs in order

You should get **exactly** the same:
- Final state
- Actions emitted
- Errors encountered

If you don't, one of these invariants is broken.

## Testing Your Invariants

```rust
#[test]
async fn test_invariants_hold() {
    let mut state = MyState::new();
    let mut rng = ChaCha8Rng::seed_from_u64(12345);
    
    for i in 0..100000 {
        let input = generate_random_input(&mut rng);
        let result = MyStateMachine::stf(&mut state, input, &mut actions).await;
        
        // Invariants must hold regardless of success/failure
        state.check_invariants()
            .expect(&format!("Invariant violated at op {}", i));
    }
}
```
