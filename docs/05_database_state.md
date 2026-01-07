# Database-Backed State

A powerful PHASM pattern: **State can be a transactional database** like FoundationDB, PostgreSQL, or SQLite.

## The Key Insight

```rust
// State is NOT just in-memory structs
struct State {
    data: HashMap<K, V>, // ❌ Limited to this
}

// State can be a database handle!
struct State {
    db: Transaction<'a>, // ✅ Transactional database
}
```

As long as the database is:
1. **Transactional** (atomic commits/rollbacks)
2. **Accessed through `state`** (not external connection)
3. **Deterministic** (same reads produce same writes)

It works perfectly with PHASM!

## Why This Works

### STF Atomicity via Database Transactions

```rust
struct BookingState<'txn> {
    txn: &'txn mut fdb::Transaction,
}

impl StateMachine for BookingSystem {
    async fn stf(
        state: &mut BookingState<'_>,
        input: Input,
        actions: &mut Actions,
    ) -> Result<(), Error> {
        // All database operations are in the transaction
        let existing = state.txn.get(b"booking/123").await?;
        
        if existing.is_some() {
            return Err(AlreadyExists); // Transaction will rollback
        }
        
        // Write to database through state
        state.txn.set(b"booking/123", booking_data);
        state.txn.set(b"pending/456", pending_data);
        
        // Emit actions
        actions.add(Action::Tracked(...))?;
        
        Ok(())
        
        // If Ok: caller commits transaction
        // If Err: caller rolls back transaction
        // Perfect atomicity!
    }
}

// Usage
async fn process_input(db: &Database, input: Input) -> Result<()> {
    let mut txn = db.create_transaction()?;
    let mut state = BookingState { txn: &mut txn };
    let mut actions = vec![];
    
    match MyStateMachine::stf(&mut state, input, &mut actions).await {
        Ok(()) => {
            txn.commit().await?; // Atomic commit
            execute_actions(&actions).await?;
            Ok(())
        }
        Err(e) => {
            // Transaction automatically rolled back on drop
            Err(e)
        }
    }
}
```

## FoundationDB Example

```rust
use foundationdb::{Database, Transaction};

struct FdbState<'txn> {
    txn: &'txn Transaction,
    // Optional: in-memory cache for this transaction
    cache: HashMap<Vec<u8>, Vec<u8>>,
}

impl<'txn> FdbState<'txn> {
    async fn get_booking(&self, id: u64) -> Result<Option<Booking>> {
        let key = format!("booking/{}", id);
        let value = self.txn.get(key.as_bytes()).await?;
        
        Ok(value.and_then(|v| bincode::deserialize(&v).ok()))
    }
    
    async fn set_booking(&self, id: u64, booking: &Booking) -> Result<()> {
        let key = format!("booking/{}", id);
        let value = bincode::serialize(booking)?;
        self.txn.set(key.as_bytes(), &value);
        Ok(())
    }
    
    async fn list_pending(&self) -> Result<Vec<(u64, PendingRequest)>> {
        let range = self.txn
            .get_range(b"pending/".., b"pending0", ..)
            .await?;
        
        range.into_iter()
            .map(|kv| {
                let id = parse_id_from_key(&kv.key())?;
                let req = bincode::deserialize(kv.value())?;
                Ok((id, req))
            })
            .collect()
    }
}

impl StateMachine for BookingSystem {
    type State = FdbState<'txn>; // Lifetime-bound to transaction
    
    async fn stf(
        state: &mut FdbState<'_>,
        input: Input,
        actions: &mut Actions,
    ) -> Result<(), Error> {
        match input {
            Input::Normal(BookingRequest { slot, user }) => {
                // Read from database through state
                let existing = state.get_booking(slot.id).await?;
                
                if existing.is_some() {
                    return Err(SlotTaken); // No writes yet, safe to error
                }
                
                // Generate ID deterministically from database state
                let counter = state.txn.get(b"counter").await?
                    .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);
                let request_id = counter + 1;
                
                // Write to database
                state.txn.set(b"counter", &request_id.to_be_bytes());
                state.set_booking(slot.id, &Booking { user, slot }).await?;
                
                let pending = PendingRequest {
                    request_id,
                    slot,
                    status: Pending,
                };
                state.txn.set(
                    format!("pending/{}", request_id).as_bytes(),
                    &bincode::serialize(&pending)?
                );
                
                // Emit tracked action
                actions.add(Action::Tracked(
                    TrackedAction::new(request_id, ChargeCard { amount: 50.0 })
                ))?;
                
                Ok(())
            }
            
            Input::TrackedActionCompleted { id, result } => {
                // Update database state
                let key = format!("pending/{}", id);
                let mut pending: PendingRequest = state.txn
                    .get(key.as_bytes())
                    .await?
                    .ok_or(NotFound)?
                    .as_ref()
                    .pipe(bincode::deserialize)?;
                
                pending.status = match result {
                    Success => Confirmed,
                    Failed => Failed,
                };
                
                state.txn.set(key.as_bytes(), &bincode::serialize(&pending)?);
                
                Ok(())
            }
        }
    }
    
    async fn restore(
        state: &FdbState<'_>,
        actions: &mut Actions,
    ) -> Result<(), ()> {
        // Restore from database state
        let pending_list = state.list_pending().await.unwrap();
        
        for (id, pending) in pending_list {
            if pending.status == Pending {
                actions.add(Action::Tracked(
                    TrackedAction::new(id, CheckStatus { id })
                ))?;
            }
        }
        
        Ok(())
    }
}
```

## PostgreSQL Example

```rust
use sqlx::{PgConnection, Transaction};

struct PgState<'c> {
    txn: Transaction<'c, Postgres>,
}

impl StateMachine for BookingSystem {
    async fn stf(
        state: &mut PgState<'_>,
        input: Input,
        actions: &mut Actions,
    ) -> Result<(), Error> {
        match input {
            Input::Normal(BookingRequest { slot, user }) => {
                // Check if slot exists (through state.txn)
                let exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM bookings WHERE slot_id = $1)"
                )
                .bind(slot.id)
                .fetch_one(&mut *state.txn)
                .await?;
                
                if exists {
                    return Err(SlotTaken); // No writes, transaction will rollback
                }
                
                // Generate ID from database sequence
                let request_id = sqlx::query_scalar::<_, i64>(
                    "SELECT nextval('request_id_seq')"
                )
                .fetch_one(&mut *state.txn)
                .await?;
                
                // Insert booking
                sqlx::query(
                    "INSERT INTO bookings (slot_id, user_id, status) VALUES ($1, $2, $3)"
                )
                .bind(slot.id)
                .bind(user)
                .bind("pending")
                .execute(&mut *state.txn)
                .await?;
                
                // Insert pending request
                sqlx::query(
                    "INSERT INTO pending_requests (id, slot_id, status) VALUES ($1, $2, $3)"
                )
                .bind(request_id)
                .bind(slot.id)
                .bind("awaiting_payment")
                .execute(&mut *state.txn)
                .await?;
                
                // Emit action
                actions.add(Action::Tracked(
                    TrackedAction::new(request_id, ChargeCard { amount: 50.0 })
                ))?;
                
                Ok(())
            }
            
            Input::TrackedActionCompleted { id, result } => {
                // Update through transaction
                let new_status = match result {
                    Success => "confirmed",
                    Failed => "failed",
                };
                
                sqlx::query(
                    "UPDATE pending_requests SET status = $1 WHERE id = $2"
                )
                .bind(new_status)
                .bind(id)
                .execute(&mut *state.txn)
                .await?;
                
                Ok(())
            }
        }
    }
    
    async fn restore(state: &PgState<'_>, actions: &mut Actions) -> Result<(), ()> {
        // Query pending from database
        let pending = sqlx::query_as::<_, (i64, String)>(
            "SELECT id, slot_id FROM pending_requests WHERE status = 'awaiting_payment'"
        )
        .fetch_all(&mut *state.txn)
        .await
        .unwrap();
        
        for (id, _slot_id) in pending {
            actions.add(Action::Tracked(
                TrackedAction::new(id, CheckPaymentStatus { id })
            )).map_err(|_| ())?;
        }
        
        Ok(())
    }
}
```

## Key Advantages

### 1. True Atomicity
Database transactions provide stronger atomicity than in-memory state:
- Crash mid-STF? Transaction rolled back automatically
- No need to manually ensure state consistency
- ACID guarantees from the database

### 2. Scalability
- State can exceed memory
- Millions of bookings/requests
- Efficient indexing and queries
- Distributed transactions (FoundationDB)

### 3. Persistence Built-In
- No separate "save state" step
- Every successful STF commits to disk
- Crash recovery is just opening the database

### 4. Concurrency
- Multiple processes can share state (with proper locking)
- Optimistic concurrency with version checks
- Database handles concurrent access

## Critical Rules

### 1. All Database Access Through State

```rust
// ❌ WRONG - external database access
async fn stf(state: &mut State, input: Input) -> Result<()> {
    let db = Database::connect("...").await?; // External!
    let data = db.query(...).await?;
}

// ✅ CORRECT - through state parameter
async fn stf(state: &mut DbState<'_>, input: Input) -> Result<()> {
    let data = state.txn.query(...).await?; // Through state
}
```

### 2. STF Must Not Commit

```rust
// ❌ WRONG - STF commits
async fn stf(state: &mut DbState<'_>, ...) -> Result<()> {
    state.txn.set(...);
    state.txn.commit().await?; // NO! Caller should commit
    Ok(())
}

// ✅ CORRECT - caller commits
async fn stf(state: &mut DbState<'_>, ...) -> Result<()> {
    state.txn.set(...);
    Ok(()) // Caller commits if Ok
}

// Caller
match stf(&mut state, input, actions).await {
    Ok(()) => state.txn.commit().await?,
    Err(e) => state.txn.rollback().await?,
}
```

### 3. Deterministic IDs from Database

```rust
// ✅ Use database sequences/counters
let id = sqlx::query_scalar("SELECT nextval('seq')").fetch_one(&mut state.txn).await?;

// ✅ Or read counter from database
let counter = state.txn.get(b"counter").await?.unwrap_or(0);
let id = counter + 1;
state.txn.set(b"counter", &id.to_be_bytes());
```

### 4. Restore Queries Database

```rust
async fn restore(state: &DbState<'_>, actions: &mut Actions) -> Result<()> {
    // ✅ Query pending operations from database
    let pending = sqlx::query!(
        "SELECT id, operation FROM pending WHERE status = 'pending'"
    )
    .fetch_all(&mut state.txn)
    .await?;
    
    for p in pending {
        actions.add(TrackedAction::new(p.id, deserialize_op(p.operation)))?;
    }
    
    Ok(())
}
```

## Pattern: Read-Your-Writes Cache

Optimize repeated reads within a transaction:

```rust
struct CachedDbState<'txn> {
    txn: &'txn Transaction,
    read_cache: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<'txn> CachedDbState<'txn> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check cache first
        if let Some(cached) = self.read_cache.get(key) {
            return Ok(cached.clone());
        }
        
        // Query database
        let value = self.txn.get(key).await?;
        
        // Cache for subsequent reads
        self.read_cache.insert(key.to_vec(), value.clone());
        
        Ok(value)
    }
    
    fn set(&mut self, key: &[u8], value: &[u8]) {
        // Update cache
        self.read_cache.insert(key.to_vec(), Some(value.to_vec()));
        
        // Write to database
        self.txn.set(key, value);
    }
}
```

## Hybrid: In-Memory + Database

Some state in memory (hot path), rest in database:

```rust
struct HybridState<'txn> {
    txn: &'txn Transaction,
    cache: LruCache<SlotId, Booking>, // Hot bookings
}

impl StateMachine for BookingSystem {
    async fn stf(state: &mut HybridState<'_>, ...) -> Result<()> {
        // Try cache first
        if let Some(booking) = state.cache.get(&slot_id) {
            return Err(SlotTaken);
        }
        
        // Fall back to database
        let exists = state.txn.get(slot_key).await?.is_some();
        if exists {
            // Populate cache for next time
            let booking = deserialize(state.txn.get(slot_key).await?);
            state.cache.put(slot_id, booking);
            return Err(SlotTaken);
        }
        
        // Insert in both
        state.txn.set(slot_key, booking_data);
        state.cache.put(slot_id, booking);
        
        Ok(())
    }
}
```

## Testing Database-Backed State

```rust
#[tokio::test]
async fn test_with_database() {
    let db = Database::connect_memory().await?; // In-memory for tests
    
    for seed in 0..100 {
        let mut txn = db.create_transaction()?;
        let mut state = DbState { txn: &mut txn };
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        
        for _ in 0..1000 {
            let input = generate_random_input(&mut rng);
            let result = MyStateMachine::stf(&mut state, input, &mut actions).await;
            
            if result.is_ok() {
                // Commit transaction
                state.txn.commit().await?;
                
                // Start new transaction for next iteration
                txn = db.create_transaction()?;
                state.txn = &mut txn;
            } else {
                // Rollback and retry with new transaction
                drop(state);
                txn = db.create_transaction()?;
                state = DbState { txn: &mut txn };
            }
            
            // Can't check in-memory invariants easily,
            // but database constraints enforce them!
        }
    }
}
```

## Database Constraints as Invariants

Let the database enforce invariants:

```sql
-- No overlapping bookings
CREATE TABLE bookings (
    slot_id BIGINT PRIMARY KEY,  -- Ensures no double-booking!
    user_id BIGINT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    duration_minutes INT NOT NULL,
    
    -- Additional constraint: no overlaps
    EXCLUDE USING gist (
        int4range(start_time, start_time + duration_minutes * INTERVAL '1 minute') WITH &&
    )
);

-- Pending requests must reference valid bookings
CREATE TABLE pending_requests (
    id BIGINT PRIMARY KEY,
    slot_id BIGINT REFERENCES bookings(slot_id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'failed'))
);
```

Now the database *enforces* your invariants! If you violate them, the transaction fails.

## Performance Considerations

- **Network latency**: Database on localhost or same datacenter
- **Transaction overhead**: Batch operations when possible
- **Index design**: Ensure queries are fast
- **Read caching**: Cache frequently-read data in the transaction
- **Connection pooling**: Reuse connections

## When to Use Database-Backed State

✅ **Use database state when:**
- State is too large for memory
- Need durability without explicit saves
- Multiple processes need shared state
- Want ACID guarantees
- Database constraints match your invariants

❌ **Use in-memory state when:**
- State is small (<1GB)
- Single-process system
- Ultra-low latency required (<1µs)
- State is ephemeral

## Conclusion

Database-backed state is a **first-class pattern** in PHASM. The transaction handle *is* your state, accessed through the `state` parameter. This provides:

- Stronger atomicity (ACID)
- Built-in persistence
- Scalability beyond memory
- Concurrent access patterns

The key is: **All database access goes through the `state` parameter**. Never open external connections in STF. The transaction is your state.
