use std::{
    future,
    pin::Pin,
    task::{Context, Poll},
};

use phasm::{
    Input, StateMachine,
    actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes},
};

/// Simulates a coffee shop loyalty app state machine.
///
/// This example demonstrates:
/// - Tracked actions: Point redemption that needs backend confirmation
/// - Untracked actions: UI updates, notifications, animations
/// - Restore: Recovering pending redemptions after crash
#[monoio::main]
async fn main() {
    println!("=== Coffee Shop Loyalty App Demo ===\n");

    // Initialize state with user having 150 points
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

    // Scenario 1: User redeems 100 points for a free coffee ($5 off)
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

    for (i, action) in actions.iter().enumerate() {
        match action {
            Action::Tracked(ta) => {
                println!("  {}. [TRACKED] {:?}", i + 1, ta);
                println!("     → Will wait for backend confirmation");
            }
            Action::Untracked(ua) => {
                println!("  {}. [UNTRACKED] {:?}", i + 1, ua);
            }
        }
    }

    actions.clear();

    // Simulate backend confirming the redemption
    println!("\n>>> Backend confirms: Redemption successful!\n");

    // Use the actual redemption ID from the pending redemption
    let redemption_id = app.pending_redemption.as_ref().unwrap().id.clone();

    CoffeeShopApp::stf(
        &mut app,
        Input::TrackedActionCompleted {
            id: redemption_id,
            res: RedemptionResult::Success {
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

    for (i, action) in actions.iter().enumerate() {
        match action {
            Action::Tracked(_) => unreachable!(),
            Action::Untracked(ua) => {
                println!("  {}. [UNTRACKED] {:?}", i + 1, ua);
            }
        }
    }

    actions.clear();

    // Scenario 2: Demonstrate error handling - user tries to redeem more points than available
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
    println!("  Actions produced: {} (empty)", actions.len());

    // Verify atomicity - state should be completely unchanged
    assert!(
        result.is_err(),
        "Should return error for insufficient points"
    );
    assert_eq!(
        app.points_balance, points_before,
        "Points should not change on error"
    );
    assert_eq!(
        app.pending_redemption, pending_before,
        "Pending should not change on error"
    );
    assert_eq!(
        app.next_redemption_id, next_id_before,
        "ID counter should not increment on error"
    );
    assert_eq!(actions.len(), 0, "No actions should be emitted on error");

    println!("\n✓ STF Atomicity verified: State unchanged after error\n");

    actions.clear();

    // Demonstrate restore functionality
    println!(">>> Simulating app crash and restore...\n");

    // Create new app state with a pending redemption (simulating crash during redemption)
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
    for (i, action) in actions.iter().enumerate() {
        match action {
            Action::Tracked(ta) => {
                println!("  {}. [TRACKED] {:?}", i + 1, ta);
                println!("     → Will requery backend to check redemption status");
            }
            Action::Untracked(_) => unreachable!(),
        }
    }

    println!("\n=== Demo Complete ===");
}

// ============================================================================
// State Machine Definition
// ============================================================================

struct CoffeeShopApp {
    user_id: u64,
    points_balance: u32,
    pending_redemption: Option<PendingRedemption>,
    order_total: f32,
    // INVARIANT: Deterministic ID generation (Invariant #4)
    // Counter must be stored in state, NOT generated from SystemTime or random
    next_redemption_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingRedemption {
    id: RedemptionId,
    #[allow(dead_code)]
    points: u32,
}

// User input to the state machine
#[derive(Debug)]
enum UserAction {
    RedeemPoints {
        points: u32,
    },
    #[allow(dead_code)]
    CancelOrder,
}

// Errors that can occur during state transitions
#[derive(Debug)]
enum CoffeeShopError {
    InsufficientPoints,
    RedemptionAlreadyPending,
    FailedToQueueAction,
    InvalidRedemptionId,
}

// ============================================================================
// Tracked Actions - Need backend confirmation
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
// Untracked Actions - Fire and forget (UI, notifications, logs)
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
    type UntrackedAction = UntrackedAction;
    type TrackedAction = CoffeeTrackedAction;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;

    type State = Self;
    type Input = UserAction;

    type TransitionError = CoffeeShopError;
    type RestoreError = ();

    type StfFuture<'state, 'actions> = CoffeeStfFuture<'state, 'actions>;
    type RestoreFuture<'state, 'actions> = future::Ready<Result<(), Self::RestoreError>>;

    fn stf<'state, 'actions>(
        state: &'state mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &'actions mut Self::Actions,
    ) -> Self::StfFuture<'state, 'actions> {
        CoffeeStfFuture {
            state,
            actions,
            input,
        }
    }

    fn restore<'state, 'actions>(
        state: &'state Self::State,
        actions: &'actions mut Self::Actions,
    ) -> Self::RestoreFuture<'state, 'actions> {
        // Clear the actions container first to reuse allocation
        let _ = actions.clear();

        // If there's a pending redemption, we need to check its status with the backend
        if let Some(pending) = &state.pending_redemption {
            // Create a tracked action to requery the backend about this redemption
            let _ = actions.add(Action::Tracked(TrackedAction::new(
                pending.id.clone(),
                RedemptionRequest::CheckStatus {
                    redemption_id: pending.id.clone(),
                },
            )));
        }

        future::ready(Ok(()))
    }
}

// ============================================================================
// State Transition Future
// ============================================================================

struct CoffeeStfFuture<'state, 'actions> {
    state: &'state mut CoffeeShopApp,
    actions: &'actions mut <CoffeeShopApp as StateMachine>::Actions,
    input: Input<
        <CoffeeShopApp as StateMachine>::TrackedAction,
        <CoffeeShopApp as StateMachine>::Input,
    >,
}

impl<'state, 'actions> Future for CoffeeStfFuture<'state, 'actions> {
    type Output = Result<(), <CoffeeShopApp as StateMachine>::TransitionError>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Extract input data before calling handlers to avoid borrow checker issues
        enum InputAction {
            RedeemPoints {
                points: u32,
            },
            CancelOrder,
            RedemptionSuccess {
                id: RedemptionId,
                points_deducted: u32,
            },
            RedemptionFailed {
                id: RedemptionId,
                reason: String,
            },
            RedemptionPending {
                id: RedemptionId,
            },
        }

        let action = match &self.input {
            Input::Normal(UserAction::RedeemPoints { points }) => {
                InputAction::RedeemPoints { points: *points }
            }
            Input::Normal(UserAction::CancelOrder) => InputAction::CancelOrder,
            Input::TrackedActionCompleted { id, res } => match res {
                RedemptionResult::Success { points_deducted } => InputAction::RedemptionSuccess {
                    id: id.clone(),
                    points_deducted: *points_deducted,
                },
                RedemptionResult::Failed { reason } => InputAction::RedemptionFailed {
                    id: id.clone(),
                    reason: reason.clone(),
                },
                RedemptionResult::Pending => InputAction::RedemptionPending { id: id.clone() },
            },
        };

        let result = match action {
            InputAction::RedeemPoints { points } => self.handle_redeem_points(points),
            InputAction::CancelOrder => self.handle_cancel_order(),
            InputAction::RedemptionSuccess {
                id,
                points_deducted,
            } => self.handle_redemption_success(&id, points_deducted),
            InputAction::RedemptionFailed { id, reason } => {
                self.handle_redemption_failed(&id, reason)
            }
            InputAction::RedemptionPending { id } => self.handle_redemption_pending(&id),
        };

        Poll::Ready(result)
    }
}

impl<'state, 'actions> CoffeeStfFuture<'state, 'actions> {
    fn handle_redeem_points(&mut self, points: u32) -> Result<(), CoffeeShopError> {
        // Check if we already have a pending redemption
        if self.state.pending_redemption.is_some() {
            return Err(CoffeeShopError::RedemptionAlreadyPending);
        }

        // Check if user has enough points
        if self.state.points_balance < points {
            return Err(CoffeeShopError::InsufficientPoints);
        }

        // Generate a deterministic redemption ID from state (don't mutate yet)
        let redemption_id = RedemptionId(self.state.next_redemption_id);

        // Perform all fallible operations BEFORE mutating state
        // Create tracked action to send to backend
        self.actions
            .add(Action::Tracked(TrackedAction::new(
                redemption_id.clone(),
                RedemptionRequest::Redeem {
                    user_id: self.state.user_id,
                    points,
                },
            )))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        // Show UI feedback (untracked - fire and forget)
        self.actions
            .add(Action::Untracked(UntrackedAction::ShowStampAnimation))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        self.actions
            .add(Action::Untracked(UntrackedAction::LogAnalytics {
                event: format!("redemption_requested:{}", points),
            }))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        // All fallible operations succeeded - NOW mutate state
        self.state.next_redemption_id += 1;

        // Store pending redemption in state (for crash recovery)
        self.state.pending_redemption = Some(PendingRedemption {
            id: redemption_id.clone(),
            points,
        });

        Ok(())
    }

    fn handle_cancel_order(&mut self) -> Result<(), CoffeeShopError> {
        // Cancel any pending redemptions
        self.state.pending_redemption = None;
        Ok(())
    }

    fn handle_redemption_success(
        &mut self,
        id: &RedemptionId,
        points_deducted: u32,
    ) -> Result<(), CoffeeShopError> {
        // Verify this is the redemption we're waiting for
        let pending = self
            .state
            .pending_redemption
            .as_ref()
            .ok_or(CoffeeShopError::InvalidRedemptionId)?;

        if &pending.id != id {
            return Err(CoffeeShopError::InvalidRedemptionId);
        }

        // Backend confirmed! Update our state
        self.state.points_balance -= points_deducted;
        let discount = (points_deducted as f32) * 0.05; // 100 points = $5
        self.state.order_total = (self.state.order_total - discount).max(0.0);
        self.state.pending_redemption = None;

        // Emit untracked actions for UI updates
        self.actions
            .add(Action::Untracked(UntrackedAction::UpdatePointsDisplay {
                new_balance: self.state.points_balance,
            }))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        self.actions
            .add(Action::Untracked(UntrackedAction::UpdateOrderTotal {
                new_total_cents: (self.state.order_total * 100.0) as u32,
            }))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        self.actions
            .add(Action::Untracked(UntrackedAction::ShowSuccessMessage {
                message: format!(
                    "Redeemed {} points! Saved ${:.2}",
                    points_deducted, discount
                ),
            }))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        self.actions
            .add(Action::Untracked(UntrackedAction::PlaySuccessSound))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        self.actions
            .add(Action::Untracked(UntrackedAction::SendPushNotification {
                message: "Your reward has been applied!".to_string(),
            }))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        Ok(())
    }

    fn handle_redemption_failed(
        &mut self,
        id: &RedemptionId,
        reason: String,
    ) -> Result<(), CoffeeShopError> {
        // Verify this is the redemption we're waiting for
        let pending = self
            .state
            .pending_redemption
            .as_ref()
            .ok_or(CoffeeShopError::InvalidRedemptionId)?;

        if &pending.id != id {
            return Err(CoffeeShopError::InvalidRedemptionId);
        }

        // Backend rejected the redemption
        self.state.pending_redemption = None;

        self.actions
            .add(Action::Untracked(UntrackedAction::ShowErrorMessage {
                message: format!("Redemption failed: {}", reason),
            }))
            .map_err(|_| CoffeeShopError::FailedToQueueAction)?;

        Ok(())
    }

    fn handle_redemption_pending(&mut self, id: &RedemptionId) -> Result<(), CoffeeShopError> {
        // Verify this is the redemption we're waiting for
        let pending = self
            .state
            .pending_redemption
            .as_ref()
            .ok_or(CoffeeShopError::InvalidRedemptionId)?;

        if &pending.id != id {
            return Err(CoffeeShopError::InvalidRedemptionId);
        }

        // Still processing, keep waiting
        Ok(())
    }
}
