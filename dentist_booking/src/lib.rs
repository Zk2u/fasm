//! Dentist booking system state machine.
//!
//! Demonstrates a realistic PHASM application with:
//! - Schedule management
//! - Slot availability checking
//! - Payment pre-authorization (tracked actions)
//! - Race condition handling
//! - Invariant checking for testing

pub mod types;

use ahash::{HashMap, HashMapExt};

use phasm::{
    actions::{Action, ActionsContainer, TrackedAction, TrackedActionTypes},
    Input, StateMachine,
};

pub use types::*;

// ============================================================================
// State Machine
// ============================================================================

pub struct BookingSystem {
    pub schedule: HashMap<Day, Vec<TimeRange>>,
    pub bookings: HashMap<Slot, ConfirmedBooking>,
    pub pending: HashMap<u64, PendingReq>,
    pub next_id: u64,
}

impl BookingSystem {
    pub fn new() -> Self {
        Self {
            schedule: HashMap::new(),
            bookings: HashMap::new(),
            pending: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn with_default_schedule() -> Self {
        let mut system = Self::new();

        // Mon: 9-12, 14-17
        system.add_schedule(
            Day::Monday,
            TimeRange::new(Time::new(9, 0), Time::new(12, 0)),
        );
        system.add_schedule(
            Day::Monday,
            TimeRange::new(Time::new(14, 0), Time::new(17, 0)),
        );

        // Tue: 9-12, 13-16
        system.add_schedule(
            Day::Tuesday,
            TimeRange::new(Time::new(9, 0), Time::new(12, 0)),
        );
        system.add_schedule(
            Day::Tuesday,
            TimeRange::new(Time::new(13, 0), Time::new(16, 0)),
        );

        // Wed: 9-12, 14-18
        system.add_schedule(
            Day::Wednesday,
            TimeRange::new(Time::new(9, 0), Time::new(12, 0)),
        );
        system.add_schedule(
            Day::Wednesday,
            TimeRange::new(Time::new(14, 0), Time::new(18, 0)),
        );

        // Thu: 10-13, 14-17
        system.add_schedule(
            Day::Thursday,
            TimeRange::new(Time::new(10, 0), Time::new(13, 0)),
        );
        system.add_schedule(
            Day::Thursday,
            TimeRange::new(Time::new(14, 0), Time::new(17, 0)),
        );

        // Fri: 9-15 (no lunch)
        system.add_schedule(
            Day::Friday,
            TimeRange::new(Time::new(9, 0), Time::new(15, 0)),
        );

        system
    }

    pub fn add_schedule(&mut self, day: Day, range: TimeRange) {
        self.schedule.entry(day).or_default().push(range);
    }

    pub fn is_available(&self, slot: Slot, dur: u16) -> bool {
        // Check schedule
        let Some(ranges) = self.schedule.get(&slot.day) else {
            return false;
        };
        if !ranges.iter().any(|r| r.can_fit(slot.time, dur)) {
            return false;
        }

        // Check conflicts
        let end = slot.time.add(dur);
        for (booked, booking) in &self.bookings {
            if booked.day != slot.day {
                continue;
            }
            let booked_end = booked.time.add(booking.apt_type.dur());
            if slot.time < booked_end && end > booked.time {
                return false;
            }
        }
        true
    }

    pub fn find_slot(&self, days: &[Day], ranges: &[TimeRange], dur: u16) -> Option<Slot> {
        for &day in days {
            let Some(sched_ranges) = self.schedule.get(&day) else {
                continue;
            };

            for sched_range in sched_ranges {
                for pref_range in ranges {
                    let start = sched_range.0.max(pref_range.0);
                    let end = sched_range.1.min(pref_range.1);
                    if start >= end {
                        continue;
                    }

                    let mut t = start;
                    while t.add(dur) <= end {
                        let slot = Slot { day, time: t };
                        if self.is_available(slot, dur) {
                            return Some(slot);
                        }
                        t = t.add(15); // Try 15-min increments
                    }
                }
            }
        }
        None
    }

    /// Check system invariants for testing
    pub fn check_invariants(&self) -> Result<(), String> {
        // 1. No overlapping bookings
        let bookings_vec: Vec<_> = self.bookings.iter().collect();
        for i in 0..bookings_vec.len() {
            for j in (i + 1)..bookings_vec.len() {
                let (slot1, booking1) = bookings_vec[i];
                let (slot2, booking2) = bookings_vec[j];

                if slot1.day == slot2.day {
                    let end1 = slot1.time.add(booking1.apt_type.dur());
                    let end2 = slot2.time.add(booking2.apt_type.dur());

                    if slot1.time < end2 && end1 > slot2.time {
                        return Err(format!(
                            "Overlapping bookings: {} ({:?}) and {} ({:?})",
                            slot1, booking1.apt_type, slot2, booking2.apt_type
                        ));
                    }
                }
            }
        }

        // 2. All bookings fit within schedule
        for (slot, booking) in &self.bookings {
            let Some(ranges) = self.schedule.get(&slot.day) else {
                return Err(format!("Booking {} on day without schedule", slot));
            };

            let fits = ranges
                .iter()
                .any(|r| r.can_fit(slot.time, booking.apt_type.dur()));
            if !fits {
                return Err(format!(
                    "Booking {} doesn't fit in schedule (dur: {})",
                    slot,
                    booking.apt_type.dur()
                ));
            }
        }

        // 3. Confirmed pending requests match bookings
        for (req_id, pending) in &self.pending {
            if pending.status == ReqStatus::SlotConfirmed {
                let Some(slot) = pending.slot else {
                    return Err(format!("Confirmed request {} has no slot", req_id));
                };

                if !self.bookings.contains_key(&slot) {
                    return Err(format!(
                        "Confirmed request {} slot {} not in bookings",
                        req_id, slot
                    ));
                }
            }
        }

        Ok(())
    }
}

impl Default for BookingSystem {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Input and Error Types
// ============================================================================

#[derive(Debug)]
pub enum BookingInput {
    RequestSlot {
        user_id: u64,
        name: String,
        email: String,
        day: Day,
        time: Time,
        apt_type: AptType,
    },
    RequestAuto {
        user_id: u64,
        name: String,
        email: String,
        days: Vec<Day>,
        times: Vec<TimeRange>,
        apt_type: AptType,
    },
}

#[derive(Debug)]
pub enum BookingError {
    SlotNotAvailable,
    NoSlotFound,
    InvalidRequest,
    ActionQueueFailed,
}

// ============================================================================
// Tracked Actions
// ============================================================================

pub type ReqId = u64;

#[derive(Debug, PartialEq, Eq)]
pub enum PaymentReq {
    Preauth {
        user_id: u64,
        amount_cents: u32,
        req_id: ReqId,
    },
    Release {
        req_id: ReqId,
    },
    CheckStatus {
        req_id: ReqId,
    },
}

#[derive(Debug)]
pub enum PaymentResult {
    Success { amount: f32 },
    Failed { reason: String },
    Released,
    Pending,
}

#[derive(Debug)]
pub struct BookingTracked;

impl TrackedActionTypes for BookingTracked {
    type Id = ReqId;
    type Action = PaymentReq;
    type Result = PaymentResult;
}

// ============================================================================
// Untracked Actions
// ============================================================================

#[derive(Debug, PartialEq, Eq)]
pub enum UntrackedAction {
    Notify { user_id: u64, msg: String },
    Log { event: String },
}

// ============================================================================
// StateMachine Implementation
// ============================================================================

impl StateMachine for BookingSystem {
    type State = Self;
    type Input = BookingInput;
    type TrackedAction = BookingTracked;
    type UntrackedAction = UntrackedAction;
    type Actions = Vec<Action<Self::UntrackedAction, Self::TrackedAction>>;
    type TransitionError = BookingError;
    type RestoreError = BookingError;

    async fn stf(
        state: &mut Self::State,
        input: Input<Self::TrackedAction, Self::Input>,
        actions: &mut Self::Actions,
    ) -> Result<(), Self::TransitionError> {
        match input {
            Input::Normal(BookingInput::RequestSlot {
                user_id,
                name,
                email,
                day,
                time,
                apt_type,
            }) => {
                let slot = Slot { day, time };
                handle_slot_request(state, actions, user_id, name, email, slot, apt_type)
            }
            Input::Normal(BookingInput::RequestAuto {
                user_id,
                name,
                email,
                days,
                times,
                apt_type,
            }) => handle_auto_request(state, actions, user_id, name, email, days, times, apt_type),
            Input::TrackedActionCompleted { id, result } => match result {
                PaymentResult::Success { amount } => {
                    handle_payment_success(state, actions, id, amount)
                }
                PaymentResult::Failed { reason } => handle_payment_failed(state, id, reason),
                PaymentResult::Released | PaymentResult::Pending => Ok(()),
            },
        }
    }

    async fn restore(
        state: &Self::State,
        actions: &mut Self::Actions,
    ) -> Result<(), Self::RestoreError> {
        for (&id, pending) in &state.pending {
            if pending.status == ReqStatus::AwaitingPreauth {
                actions
                    .add(Action::Tracked(TrackedAction::new(
                        id,
                        PaymentReq::CheckStatus { req_id: id },
                    )))
                    .map_err(|_| BookingError::ActionQueueFailed)?;
            }
        }

        Ok(())
    }
}

// ============================================================================
// Handler Functions
// ============================================================================

fn handle_slot_request(
    state: &mut BookingSystem,
    actions: &mut Vec<Action<UntrackedAction, BookingTracked>>,
    user_id: u64,
    name: String,
    email: String,
    slot: Slot,
    apt_type: AptType,
) -> Result<(), BookingError> {
    // Validation
    if !state.is_available(slot, apt_type.dur()) {
        return Err(BookingError::SlotNotAvailable);
    }

    // Prepare values (no mutation yet)
    let id = state.next_id;

    // Fallible operation first
    actions
        .add(Action::Tracked(TrackedAction::new(
            id,
            PaymentReq::Preauth {
                user_id,
                amount_cents: (apt_type.price() * 100.0) as u32,
                req_id: id,
            },
        )))
        .map_err(|_| BookingError::ActionQueueFailed)?;

    // Now mutate state
    state.next_id += 1;
    state.pending.insert(
        id,
        PendingReq {
            user_id,
            name,
            email,
            slot: Some(slot),
            apt_type,
            status: ReqStatus::AwaitingPreauth,
        },
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_auto_request(
    state: &mut BookingSystem,
    actions: &mut Vec<Action<UntrackedAction, BookingTracked>>,
    user_id: u64,
    name: String,
    email: String,
    days: Vec<Day>,
    times: Vec<TimeRange>,
    apt_type: AptType,
) -> Result<(), BookingError> {
    // Find available slot
    let slot = state
        .find_slot(&days, &times, apt_type.dur())
        .ok_or(BookingError::NoSlotFound)?;

    // Prepare values (no mutation yet)
    let id = state.next_id;

    // Fallible operation first
    actions
        .add(Action::Tracked(TrackedAction::new(
            id,
            PaymentReq::Preauth {
                user_id,
                amount_cents: (apt_type.price() * 100.0) as u32,
                req_id: id,
            },
        )))
        .map_err(|_| BookingError::ActionQueueFailed)?;

    // Now mutate state
    state.next_id += 1;
    state.pending.insert(
        id,
        PendingReq {
            user_id,
            name,
            email,
            slot: Some(slot),
            apt_type,
            status: ReqStatus::AwaitingPreauth,
        },
    );

    Ok(())
}

fn handle_payment_success(
    state: &mut BookingSystem,
    actions: &mut Vec<Action<UntrackedAction, BookingTracked>>,
    req_id: ReqId,
    amount: f32,
) -> Result<(), BookingError> {
    let pending = state
        .pending
        .get(&req_id)
        .ok_or(BookingError::InvalidRequest)?;

    let slot = pending.slot.ok_or(BookingError::InvalidRequest)?;
    let apt_type = pending.apt_type;
    let user_id = pending.user_id;
    let name = pending.name.clone();
    let email = pending.email.clone();

    // Race condition check - slot may have been taken
    if !state.is_available(slot, apt_type.dur()) {
        let pending = state.pending.get_mut(&req_id).unwrap();
        pending.status = ReqStatus::SlotTaken;

        // Release the pre-auth
        let _ = actions.add(Action::Tracked(TrackedAction::new(
            req_id,
            PaymentReq::Release { req_id },
        )));

        return Ok(());
    }

    // Confirm booking
    let pending = state.pending.get_mut(&req_id).unwrap();
    pending.status = ReqStatus::SlotConfirmed;

    state.bookings.insert(
        slot,
        ConfirmedBooking {
            user_id,
            name,
            email,
            apt_type,
            amount_paid: amount,
        },
    );

    Ok(())
}

fn handle_payment_failed(
    state: &mut BookingSystem,
    req_id: ReqId,
    _reason: String,
) -> Result<(), BookingError> {
    if let Some(pending) = state.pending.get_mut(&req_id) {
        pending.status = ReqStatus::NoSlot;
    }
    Ok(())
}
