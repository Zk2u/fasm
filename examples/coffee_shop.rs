//! Coffee shop loyalty server state machine example.
//!
//! This example demonstrates a **server-side** loyalty system that:
//! - Tracks all users' point balances (source of truth)
//! - Processes redemption requests from mobile app clients
//! - Coordinates with an external POS system to apply discounts (tracked action)
//! - Sends push notifications to users (untracked action)
//!
//! ## Why Server-Side?
//!
//! FASM is ideal for server-side state machines because:
//! - The server IS the source of truth (not a cache of remote data)
//! - Crash recovery is critical (can't lose redemption records)
//! - External system coordination (POS) needs tracked actions
//! - IDs are generated authoritatively
//!
//! ## Architecture
//!
//! ```text
//! Mobile App ──► Loyalty Server (this state machine) ──► POS System
//!                      │
//!                      └──► Push Notification Service
//! ```
//!
//! ## Tracked vs Untracked Actions
//!
//! - **Tracked**: `ApplyDiscount` to POS - we MUST know if it succeeded to maintain
//!   consistency between points balance and actual discounts applied
//! - **Untracked**: Push notifications - nice to have, but not critical for correctness
//!
//! ## Restore Pattern
//!
//! After a crash, we use `CheckDiscountStatus` (not `ApplyDiscount`) because the POS
//! is not idempotent - re-sending `ApplyDiscount` could apply the discount twice!

use std::collections::HashMap;

use fasm::{
    Input, StateMachine,
    actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes},
};

// ============================================================================
// Main Demo
// ============================================================================

#[monoio::main]
async fn main() {
    println!("=== Coffee Shop Loyalty Server Demo ===\n");

    let mut server = LoyaltyServer::new();
    let mut actions = Vec::new();

    // Setup: Create some user accounts with initial points
    println!("Setting up users...");
    LoyaltyServer::stf(
        &mut server,
        Input::Normal(ClientRequest::CreateAccount { user_id: 1001 }),
        &mut actions,
    )
    .await
    .unwrap();
    actions.clear();

    LoyaltyServer::stf(
        &mut server,
        Input::Normal(ClientRequest::AddPoints {
            user_id: 1001,
            points: 150,
            reason: "Welcome bonus".into(),
        }),
        &mut actions,
    )
    .await
    .unwrap();

    println!("User 1001 created with 150 points\n");
    print_actions(&actions);
    actions.clear();

    // Scenario 1: Successful redemption flow
    println!("\n>>> Mobile app requests: Redeem 100 points on order #5001\n");

    LoyaltyServer::stf(
        &mut server,
        Input::Normal(ClientRequest::RedeemPoints {
            user_id: 1001,
            order_id: 5001,
            points: 100,
        }),
        &mut actions,
    )
    .await
    .unwrap();

    println!("Server state after redemption request:");
    println!(
        "  User 1001 points: {} (deducted optimistically)",
        server.user_points.get(&1001).unwrap()
    );
    println!(
        "  Pending redemptions: {}",
        server.pending_redemptions.len()
    );
    println!("\nActions to execute:");
    print_actions(&actions);

    // Simulate POS confirming the discount
    let redemption_id = server.pending_redemptions.keys().next().copied().unwrap();
    actions.clear();

    println!("\n>>> POS system confirms: Discount applied to order #5001\n");

    LoyaltyServer::stf(
        &mut server,
        Input::TrackedActionCompleted {
            id: redemption_id,
            result: PosResult::DiscountApplied { order_id: 5001 },
        },
        &mut actions,
    )
    .await
    .unwrap();

    println!("Server state after POS confirmation:");
    println!(
        "  User 1001 points: {}",
        server.user_points.get(&1001).unwrap()
    );
    println!(
        "  Pending redemptions: {} (completed)",
        server.pending_redemptions.len()
    );
    println!("\nActions to execute:");
    print_actions(&actions);
    actions.clear();

    // Scenario 2: Failed redemption (insufficient points)
    println!("\n>>> Mobile app requests: Redeem 100 points (user only has 50)\n");

    let result = LoyaltyServer::stf(
        &mut server,
        Input::Normal(ClientRequest::RedeemPoints {
            user_id: 1001,
            order_id: 5002,
            points: 100,
        }),
        &mut actions,
    )
    .await;

    println!("Result: {:?}", result);
    println!(
        "User 1001 points: {} (unchanged)",
        server.user_points.get(&1001).unwrap()
    );
    actions.clear();

    // Scenario 3: POS rejection (order already closed)
    println!("\n>>> New redemption request for 30 points on order #5003\n");

    LoyaltyServer::stf(
        &mut server,
        Input::Normal(ClientRequest::RedeemPoints {
            user_id: 1001,
            order_id: 5003,
            points: 30,
        }),
        &mut actions,
    )
    .await
    .unwrap();

    let redemption_id = server.pending_redemptions.keys().next().copied().unwrap();
    println!(
        "Points deducted optimistically: {}",
        server.user_points.get(&1001).unwrap()
    );
    actions.clear();

    println!("\n>>> POS system rejects: Order #5003 already closed\n");

    LoyaltyServer::stf(
        &mut server,
        Input::TrackedActionCompleted {
            id: redemption_id,
            result: PosResult::Failed {
                reason: "Order already closed".into(),
            },
        },
        &mut actions,
    )
    .await
    .unwrap();

    println!("Server state after POS rejection:");
    println!(
        "  User 1001 points: {} (refunded!)",
        server.user_points.get(&1001).unwrap()
    );
    println!("\nActions to execute:");
    print_actions(&actions);
    actions.clear();

    // Scenario 4: Crash recovery simulation
    println!("\n>>> Simulating server crash with pending redemption...\n");

    // Create a new redemption that will be "in flight" when we crash
    LoyaltyServer::stf(
        &mut server,
        Input::Normal(ClientRequest::RedeemPoints {
            user_id: 1001,
            order_id: 5004,
            points: 20,
        }),
        &mut actions,
    )
    .await
    .unwrap();
    actions.clear();

    println!("State before crash:");
    println!(
        "  User 1001 points: {}",
        server.user_points.get(&1001).unwrap()
    );
    println!(
        "  Pending redemptions: {}",
        server.pending_redemptions.len()
    );

    // Simulate crash & restore
    println!("\n💥 CRASH! Server restarts and calls restore()...\n");

    LoyaltyServer::restore(&server, &mut actions).await.unwrap();

    println!(
        "Restore produced {} action(s) to check POS status:",
        actions.len()
    );
    print_actions(&actions);

    println!("\n=== Demo Complete ===");
}

fn print_actions(actions: &[Action<Notification, PosTracked>]) {
    if actions.is_empty() {
        println!("  (none)");
        return;
    }
    for (i, action) in actions.iter().enumerate() {
        match action {
            Action::Tracked(ta) => {
                println!("  {}. [TRACKED → POS] {:?}", i + 1, ta.action());
            }
            Action::Untracked(notif) => {
                println!("  {}. [UNTRACKED] {:?}", i + 1, notif);
            }
        }
    }
}

// ============================================================================
// Server State
// ============================================================================

/// The loyalty server - source of truth for all users' points.
struct LoyaltyServer {
    /// All users' point balances. The server is authoritative.
    user_points: HashMap<u64, u32>,

    /// Redemptions waiting for POS confirmation.
    /// Key is the redemption ID (server-generated).
    pending_redemptions: HashMap<u64, PendingRedemption>,

    /// Next redemption ID. Server generates all IDs.
    next_redemption_id: u64,
}

/// A redemption request awaiting POS confirmation.
#[derive(Debug, Clone)]
#[allow(dead_code)] // order_id kept for debugging/logging purposes
struct PendingRedemption {
    user_id: u64,
    order_id: u64,
    points: u32,
    discount_cents: u32,
}

impl LoyaltyServer {
    fn new() -> Self {
        Self {
            user_points: HashMap::new(),
            pending_redemptions: HashMap::new(),
            next_redemption_id: 1,
        }
    }

    /// Calculate discount: 5 cents per point (100 points = $5.00)
    fn points_to_discount_cents(points: u32) -> u32 {
        points * 5
    }
}

// ============================================================================
// Input Types (from mobile apps, webhooks, admin)
// ============================================================================

/// Requests that can come into the loyalty server.
#[derive(Debug)]
enum ClientRequest {
    /// Mobile app: User wants to redeem points for a discount.
    RedeemPoints {
        user_id: u64,
        order_id: u64,
        points: u32,
    },

    /// Webhook from POS: User made a purchase, award points.
    AddPoints {
        user_id: u64,
        points: u32,
        reason: String,
    },

    /// New user signed up.
    CreateAccount { user_id: u64 },
}

/// Errors from the loyalty server.
#[derive(Debug)]
enum LoyaltyError {
    UserNotFound,
    InsufficientPoints,
    UserAlreadyExists,
    RedemptionNotFound,
    ActionQueueFailed,
}

// ============================================================================
// Tracked Actions (to external POS system)
// ============================================================================

/// Unique ID for a redemption request (server-generated).
type RedemptionId = u64;

/// Commands sent to the external POS system.
///
/// The POS system is NOT part of our state machine - it's an external system
/// that we coordinate with via tracked actions.
#[derive(Debug, PartialEq, Eq)]
enum PosCommand {
    /// Tell POS to apply a discount to an order.
    /// Emitted during normal redemption flow.
    ApplyDiscount {
        order_id: u64,
        discount_cents: u32,
        redemption_id: RedemptionId,
    },

    /// Query POS for status of a pending discount.
    /// Emitted during restore - we can't re-send ApplyDiscount because
    /// the POS might apply it twice!
    CheckDiscountStatus { redemption_id: RedemptionId },
}

/// Results from the POS system.
#[derive(Debug)]
enum PosResult {
    /// POS successfully applied the discount.
    DiscountApplied { order_id: u64 },

    /// POS rejected the discount (order closed, invalid, etc.)
    Failed { reason: String },

    /// POS says the operation is still pending (for CheckDiscountStatus).
    #[allow(dead_code)] // Included for API completeness - runtime would handle retries
    Pending,
}

/// Tracked action type definitions.
#[derive(Debug)]
struct PosTracked;

impl TrackedActionTypes for PosTracked {
    type Id = RedemptionId;
    type Action = PosCommand;
    type Result = PosResult;
}

// ============================================================================
// Untracked Actions (notifications, logging)
// ============================================================================

/// Fire-and-forget notifications. Not critical for correctness.
#[derive(Debug, PartialEq, Eq)]
enum Notification {
    /// Send push notification to user's mobile app.
    PushNotification {
        user_id: u64,
        title: String,
        message: String,
    },

    /// Log analytics event.
    LogEvent { event: String },
}

// ============================================================================
// State Machine Implementation
// ============================================================================

impl StateMachine for LoyaltyServer {
    type State = Self;
    type Input = ClientRequest;
    type TrackedAction = PosTracked;
    type UntrackedAction = Notification;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type TransitionError = LoyaltyError;
    type RestoreError = LoyaltyError;

    async fn stf<'state, 'actions>(
        state: &'state mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'actions mut Self::Actions,
    ) -> Result<(), Self::TransitionError> {
        match input {
            Input::Normal(ClientRequest::CreateAccount { user_id }) => {
                handle_create_account(state, actions, user_id)
            }

            Input::Normal(ClientRequest::AddPoints {
                user_id,
                points,
                reason,
            }) => handle_add_points(state, actions, user_id, points, reason),

            Input::Normal(ClientRequest::RedeemPoints {
                user_id,
                order_id,
                points,
            }) => handle_redeem_points(state, actions, user_id, order_id, points),

            Input::TrackedActionCompleted { id, result } => {
                handle_pos_result(state, actions, id, result)
            }
        }
    }

    async fn restore<'state, 'actions>(
        state: &'state Self::State,
        actions: &'actions mut Self::Actions,
    ) -> Result<(), Self::RestoreError> {
        // After a crash, we have pending redemptions but don't know if the POS
        // applied the discounts. We use CheckDiscountStatus (NOT ApplyDiscount)
        // because the POS is non-idempotent - re-sending ApplyDiscount could
        // apply the discount twice!
        //
        // If CheckDiscountStatus returns Pending, the runtime should retry later
        // (with exponential backoff or a job queue - not shown in this example).

        for (&redemption_id, _pending) in &state.pending_redemptions {
            actions
                .add(Action::Tracked(TrackedAction::new(
                    redemption_id,
                    PosCommand::CheckDiscountStatus { redemption_id },
                )))
                .map_err(|_| LoyaltyError::ActionQueueFailed)?;
        }

        Ok(())
    }
}

// ============================================================================
// Handler Functions
// ============================================================================

fn handle_create_account(
    state: &mut LoyaltyServer,
    actions: &mut Vec<Action<Notification, PosTracked>>,
    user_id: u64,
) -> Result<(), LoyaltyError> {
    // Validation
    if state.user_points.contains_key(&user_id) {
        return Err(LoyaltyError::UserAlreadyExists);
    }

    // Mutate state
    state.user_points.insert(user_id, 0);

    // Log event (untracked - fire and forget)
    actions
        .add(Action::Untracked(Notification::LogEvent {
            event: format!("account_created:user_{}", user_id),
        }))
        .map_err(|_| LoyaltyError::ActionQueueFailed)?;

    Ok(())
}

fn handle_add_points(
    state: &mut LoyaltyServer,
    actions: &mut Vec<Action<Notification, PosTracked>>,
    user_id: u64,
    points: u32,
    reason: String,
) -> Result<(), LoyaltyError> {
    // Validation
    let balance = state
        .user_points
        .get_mut(&user_id)
        .ok_or(LoyaltyError::UserNotFound)?;

    // Mutate state
    *balance += points;
    let new_balance = *balance;

    // Notify user (untracked)
    actions
        .add(Action::Untracked(Notification::PushNotification {
            user_id,
            title: "Points earned! ☕".into(),
            message: format!(
                "You earned {} points for: {}. New balance: {}",
                points, reason, new_balance
            ),
        }))
        .map_err(|_| LoyaltyError::ActionQueueFailed)?;

    actions
        .add(Action::Untracked(Notification::LogEvent {
            event: format!("points_added:user_{}:{}:{}", user_id, points, reason),
        }))
        .map_err(|_| LoyaltyError::ActionQueueFailed)?;

    Ok(())
}

fn handle_redeem_points(
    state: &mut LoyaltyServer,
    actions: &mut Vec<Action<Notification, PosTracked>>,
    user_id: u64,
    order_id: u64,
    points: u32,
) -> Result<(), LoyaltyError> {
    // Validation
    let balance = state
        .user_points
        .get(&user_id)
        .ok_or(LoyaltyError::UserNotFound)?;

    if *balance < points {
        return Err(LoyaltyError::InsufficientPoints);
    }

    // Prepare values (no mutation yet)
    let redemption_id = state.next_redemption_id;
    let discount_cents = LoyaltyServer::points_to_discount_cents(points);

    // Fallible operation first: queue the tracked action to POS
    actions
        .add(Action::Tracked(TrackedAction::new(
            redemption_id,
            PosCommand::ApplyDiscount {
                order_id,
                discount_cents,
                redemption_id,
            },
        )))
        .map_err(|_| LoyaltyError::ActionQueueFailed)?;

    // Now mutate state (point of no return)
    state.next_redemption_id += 1;

    // Deduct points optimistically - we'll refund if POS rejects
    *state.user_points.get_mut(&user_id).unwrap() -= points;

    // Store pending redemption for tracking
    state.pending_redemptions.insert(
        redemption_id,
        PendingRedemption {
            user_id,
            order_id,
            points,
            discount_cents,
        },
    );

    Ok(())
}

fn handle_pos_result(
    state: &mut LoyaltyServer,
    actions: &mut Vec<Action<Notification, PosTracked>>,
    redemption_id: RedemptionId,
    result: PosResult,
) -> Result<(), LoyaltyError> {
    let pending = state
        .pending_redemptions
        .get(&redemption_id)
        .ok_or(LoyaltyError::RedemptionNotFound)?;

    let user_id = pending.user_id;
    let points = pending.points;
    let discount_cents = pending.discount_cents;

    match result {
        PosResult::DiscountApplied { order_id } => {
            // Success! Remove from pending (points already deducted)
            state.pending_redemptions.remove(&redemption_id);

            // Notify user
            actions
                .add(Action::Untracked(Notification::PushNotification {
                    user_id,
                    title: "Discount applied! 🎉".into(),
                    message: format!(
                        "You saved ${:.2} on order #{}!",
                        discount_cents as f32 / 100.0,
                        order_id
                    ),
                }))
                .map_err(|_| LoyaltyError::ActionQueueFailed)?;

            actions
                .add(Action::Untracked(Notification::LogEvent {
                    event: format!(
                        "redemption_complete:user_{}:order_{}:points_{}",
                        user_id, order_id, points
                    ),
                }))
                .map_err(|_| LoyaltyError::ActionQueueFailed)?;
        }

        PosResult::Failed { reason } => {
            // POS rejected - refund points to user
            state.pending_redemptions.remove(&redemption_id);
            *state.user_points.get_mut(&user_id).unwrap() += points;

            // Notify user of failure
            actions
                .add(Action::Untracked(Notification::PushNotification {
                    user_id,
                    title: "Redemption failed".into(),
                    message: format!(
                        "We couldn't apply your discount: {}. Your {} points have been refunded.",
                        reason, points
                    ),
                }))
                .map_err(|_| LoyaltyError::ActionQueueFailed)?;

            actions
                .add(Action::Untracked(Notification::LogEvent {
                    event: format!(
                        "redemption_failed:user_{}:redemption_{}:{}",
                        user_id, redemption_id, reason
                    ),
                }))
                .map_err(|_| LoyaltyError::ActionQueueFailed)?;
        }

        PosResult::Pending => {
            // POS still processing. In production, the runtime should schedule
            // another CheckDiscountStatus with exponential backoff.
            // For now, we do nothing - the redemption stays pending.
            actions
                .add(Action::Untracked(Notification::LogEvent {
                    event: format!(
                        "redemption_pending:user_{}:redemption_{}",
                        user_id, redemption_id
                    ),
                }))
                .map_err(|_| LoyaltyError::ActionQueueFailed)?;
        }
    }

    Ok(())
}
