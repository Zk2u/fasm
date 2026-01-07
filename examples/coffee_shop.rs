//! Coffee shop loyalty app state machine example.
//!
//! This example demonstrates:
//! - Tracked actions: Point redemption that needs backend confirmation
//! - Untracked actions: UI updates, notifications, animations
//! - Restore: Recovering pending redemptions after crash
//! - Atomicity: State unchanged on error

use phasm::{
    Input, StateMachine,
    actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes},
};

#[monoio::main]
async fn main() {
    println!("=== Coffee Shop Loyalty App Demo ===\n");

    let mut app = CoffeeShopApp {
        user_id: 12345,
        points_balance: 150,
        pending_redemption: None,
        order_total: 5.50,
        next_redemption_id: 1,
    };

    let mut actions = Vec::new();

    println!("Initial state:");
    println!("  Points: {}", app.points_balance);
    println!("  Order total: ${:.2}", app.order_total);
    println!("  Pending redemption: {:?}\n", app.pending_redemption);

    // Scenario 1: User redeems 100 points
    println!(">>> User taps 'Redeem 100 points for $5 off'\n");

    CoffeeShopApp::stf(
        &mut app,
        Input::Normal(UserAction::RedeemPoints { points: 100 }),
        &mut actions,
    )
    .await
    .unwrap();

    println!("After redemption request:");
    println!(
        "  Points: {} (locked, pending confirmation)",
        app.points_balance
    );
    println!("  Pending redemption: {:?}", app.pending_redemption);
    println!("\nActions produced:");
    print_actions(&actions);
    actions.clear();

    // Backend confirms the redemption
    println!("\n>>> Backend confirms: Redemption successful!\n");

    let redemption_id = app.pending_redemption.as_ref().unwrap().id.clone();

    CoffeeShopApp::stf(
        &mut app,
        Input::TrackedActionCompleted {
            id: redemption_id,
            result: RedemptionResult::Success {
                points_deducted: 100,
            },
        },
        &mut actions,
    )
    .await
    .unwrap();

    println!("After redemption confirmed:");
    println!("  Points: {}", app.points_balance);
    println!("  Order total: ${:.2}", app.order_total);
    println!("  Pending redemption: {:?}", app.pending_redemption);
    println!("\nActions produced:");
    print_actions(&actions);
    actions.clear();

    // Scenario 2: Error handling - insufficient points
    println!("\n>>> User tries to redeem 200 points (only has 50 remaining)...\n");

    let points_before = app.points_balance;
    let pending_before = app.pending_redemption.clone();
    let next_id_before = app.next_redemption_id;

    let result = CoffeeShopApp::stf(
        &mut app,
        Input::Normal(UserAction::RedeemPoints { points: 200 }),
        &mut actions,
    )
    .await;

    println!("Result: {:?}", result);
    println!("\nState after error (unchanged due to atomicity):");
    println!("  Points: {} (same as before)", app.points_balance);
    println!(
        "  Pending redemption: {:?} (same as before)",
        app.pending_redemption
    );
    println!(
        "  Next redemption ID: {} (same as before)",
        app.next_redemption_id
    );

    // Verify atomicity
    assert!(result.is_err());
    assert_eq!(app.points_balance, points_before);
    assert_eq!(app.pending_redemption, pending_before);
    assert_eq!(app.next_redemption_id, next_id_before);
    assert_eq!(actions.len(), 0);

    println!("\n✓ STF Atomicity verified: State unchanged after error\n");
    actions.clear();

    // Scenario 3: Restore after crash
    println!(">>> Simulating app crash and restore...\n");

    let crashed_app = CoffeeShopApp {
        user_id: 12345,
        points_balance: 150,
        pending_redemption: Some(PendingRedemption {
            id: RedemptionId(2),
            points: 100,
        }),
        order_total: 5.50,
        next_redemption_id: 3,
    };

    println!("Crashed state recovered from disk:");
    println!("  Points: {}", crashed_app.points_balance);
    println!("  Pending redemption: {:?}", crashed_app.pending_redemption);

    CoffeeShopApp::restore(&crashed_app, &mut actions)
        .await
        .unwrap();

    println!("\nRestore produced {} action(s) to retry:", actions.len());
    print_actions(&actions);

    println!("\n=== Demo Complete ===");
}

fn print_actions(actions: &[Action<UntrackedAction, CoffeeTrackedAction>]) {
    for (i, action) in actions.iter().enumerate() {
        match action {
            Action::Tracked(ta) => {
                println!("  {}. [TRACKED] {:?}", i + 1, ta);
            }
            Action::Untracked(ua) => {
                println!("  {}. [UNTRACKED] {:?}", i + 1, ua);
            }
        }
    }
}

// ============================================================================
// State Machine Definition
// ============================================================================

struct CoffeeShopApp {
    user_id: u64,
    points_balance: u32,
    pending_redemption: Option<PendingRedemption>,
    order_total: f32,
    next_redemption_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingRedemption {
    id: RedemptionId,
    points: u32,
}

#[derive(Debug)]
enum UserAction {
    RedeemPoints {
        points: u32,
    },
    #[allow(dead_code)]
    CancelOrder,
}

#[derive(Debug)]
enum CoffeeShopError {
    InsufficientPoints,
    RedemptionAlreadyPending,
    FailedToQueueAction,
    InvalidRedemptionId,
}

// ============================================================================
// Tracked Actions
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RedemptionId(u64);

#[derive(Debug, PartialEq, Eq)]
enum RedemptionRequest {
    Redeem { user_id: u64, points: u32 },
    CheckStatus { redemption_id: RedemptionId },
}

#[derive(Debug)]
enum RedemptionResult {
    Success {
        points_deducted: u32,
    },
    #[allow(dead_code)]
    Failed {
        reason: String,
    },
    #[allow(dead_code)]
    Pending,
}

#[derive(Debug)]
struct CoffeeTrackedAction;

impl TrackedActionTypes for CoffeeTrackedAction {
    type Id = RedemptionId;
    type Action = RedemptionRequest;
    type Result = RedemptionResult;
}

// ============================================================================
// Untracked Actions
// ============================================================================

#[derive(Debug, PartialEq, Eq)]
enum UntrackedAction {
    ShowStampAnimation,
    UpdatePointsDisplay { new_balance: u32 },
    UpdateOrderTotal { new_total_cents: u32 },
    ShowSuccessMessage { message: String },
    ShowErrorMessage { message: String },
    PlaySuccessSound,
    SendPushNotification { message: String },
    LogAnalytics { event: String },
}

// ============================================================================
// StateMachine Implementation
// ============================================================================

impl StateMachine for CoffeeShopApp {
    type State = Self;
    type Input = UserAction;
    type TrackedAction = CoffeeTrackedAction;
    type UntrackedAction = UntrackedAction;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type TransitionError = CoffeeShopError;
    type RestoreError = CoffeeShopError;

    async fn stf<'state, 'actions>(
        state: &'state mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'actions mut Self::Actions,
    ) -> Result<(), Self::TransitionError> {
        match input {
            Input::Normal(UserAction::RedeemPoints { points }) => {
                handle_redeem_points(state, actions, points)
            }
            Input::Normal(UserAction::CancelOrder) => {
                state.pending_redemption = None;
                Ok(())
            }
            Input::TrackedActionCompleted { id, result } => match result {
                RedemptionResult::Success { points_deducted } => {
                    handle_redemption_success(state, actions, &id, points_deducted)
                }
                RedemptionResult::Failed { reason } => {
                    handle_redemption_failed(state, actions, &id, reason)
                }
                RedemptionResult::Pending => {
                    // Verify ID matches, otherwise no-op
                    let pending = state
                        .pending_redemption
                        .as_ref()
                        .ok_or(CoffeeShopError::InvalidRedemptionId)?;
                    if &pending.id != &id {
                        return Err(CoffeeShopError::InvalidRedemptionId);
                    }
                    Ok(())
                }
            },
        }
    }

    async fn restore<'state, 'actions>(
        state: &'state Self::State,
        actions: &'actions mut Self::Actions,
    ) -> Result<(), Self::RestoreError> {
        if let Some(pending) = &state.pending_redemption {
            actions
                .add(Action::Tracked(TrackedAction::new(
                    pending.id.clone(),
                    RedemptionRequest::CheckStatus {
                        redemption_id: pending.id.clone(),
                    },
                )))
                .map_err(|_| CoffeeShopError::FailedToQueueAction)?;
        }

        Ok(())
    }
}

// ============================================================================
// Handler Functions
// ============================================================================

fn handle_redeem_points(
    state: &mut CoffeeShopApp,
    actions: &mut Vec<Action<UntrackedAction, CoffeeTrackedAction>>,
    points: u32,
) -> Result<(), CoffeeShopError> {
    // Validation
    if state.pending_redemption.is_some() {
        return Err(CoffeeShopError::RedemptionAlreadyPending);
    }
    if state.points_balance < points {
        return Err(CoffeeShopError::InsufficientPoints);
    }

    // Prepare values (no mutation yet)
    let redemption_id = RedemptionId(state.next_redemption_id);

    // Fallible operations first
    actions
        .add(Action::Tracked(TrackedAction::new(
            redemption_id.clone(),
            RedemptionRequest::Redeem {
                user_id: state.user_id,
                points,
            },
        )))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    actions
        .add(Action::Untracked(UntrackedAction::ShowStampAnimation))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    actions
        .add(Action::Untracked(UntrackedAction::LogAnalytics {
            event: format!("redemption_requested:{}", points),
        }))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    // Now mutate state
    state.next_redemption_id += 1;
    state.pending_redemption = Some(PendingRedemption {
        id: redemption_id,
        points,
    });

    Ok(())
}

fn handle_redemption_success(
    state: &mut CoffeeShopApp,
    actions: &mut Vec<Action<UntrackedAction, CoffeeTrackedAction>>,
    id: &RedemptionId,
    points_deducted: u32,
) -> Result<(), CoffeeShopError> {
    // Validate
    let pending = state
        .pending_redemption
        .as_ref()
        .ok_or(CoffeeShopError::InvalidRedemptionId)?;
    if &pending.id != id {
        return Err(CoffeeShopError::InvalidRedemptionId);
    }

    // Update state
    state.points_balance -= points_deducted;
    let discount = (points_deducted as f32) * 0.05;
    state.order_total = (state.order_total - discount).max(0.0);
    state.pending_redemption = None;

    // Emit UI actions
    actions
        .add(Action::Untracked(UntrackedAction::UpdatePointsDisplay {
            new_balance: state.points_balance,
        }))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    actions
        .add(Action::Untracked(UntrackedAction::UpdateOrderTotal {
            new_total_cents: (state.order_total * 100.0) as u32,
        }))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    actions
        .add(Action::Untracked(UntrackedAction::ShowSuccessMessage {
            message: format!(
                "Redeemed {} points! Saved ${:.2}",
                points_deducted, discount
            ),
        }))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    actions
        .add(Action::Untracked(UntrackedAction::PlaySuccessSound))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    actions
        .add(Action::Untracked(UntrackedAction::SendPushNotification {
            message: "Your reward has been applied!".to_string(),
        }))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    Ok(())
}

fn handle_redemption_failed(
    state: &mut CoffeeShopApp,
    actions: &mut Vec<Action<UntrackedAction, CoffeeTrackedAction>>,
    id: &RedemptionId,
    reason: String,
) -> Result<(), CoffeeShopError> {
    // Validate
    let pending = state
        .pending_redemption
        .as_ref()
        .ok_or(CoffeeShopError::InvalidRedemptionId)?;
    if &pending.id != id {
        return Err(CoffeeShopError::InvalidRedemptionId);
    }

    // Update state
    state.pending_redemption = None;

    // Show error
    actions
        .add(Action::Untracked(UntrackedAction::ShowErrorMessage {
            message: format!("Redemption failed: {}", reason),
        }))
        .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

    Ok(())
}
