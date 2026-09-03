//! Target-aware marker bounds for native and browser storage implementations.

/// A portability bound that requires [`Send`] on native targets.
///
/// Native executors move storage sessions and their futures between tasks, so
/// those values must be `Send`. Browser IndexedDB implementations may instead
/// hold JavaScript values that cannot cross threads. Defining that difference
/// here keeps conditional bounds out of the storage traits and preserves their
/// native contract.
///
/// Native `async fn` trait implementations need no target-specific attribute;
/// use this marker on explicit future bounds that must retain the native
/// `Send` contract while permitting thread-local browser futures.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait MaybeSend: Send {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send> MaybeSend for T {}

/// A portability bound that does not require [`Send`] in browsers.
///
/// IndexedDB handles can contain `JsValue`s, which are not `Send`. The browser
/// form therefore accepts every type while native builds retain the `Send`
/// supertrait. This applies only to `wasm32-unknown-unknown` browsers, not all
/// WebAssembly targets: `wasm32-wasip1-threads`, for example, has real threads.
/// Keeping the distinction in this marker leaves the native storage contract
/// intact.
///
/// Native `async fn` trait implementations need no target-specific attribute;
/// use this marker on explicit future bounds that may be `!Send` only in the
/// browser.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait MaybeSend {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T> MaybeSend for T {}

/// A portability bound that requires [`Sync`] on native targets.
///
/// Native executors may share storage sessions between tasks, so session and
/// error values at storage boundaries must be `Sync`. Browser IndexedDB
/// handles may contain JavaScript values that are not `Sync`; centralizing the
/// conditional requirement here keeps the native contract unchanged.
///
/// The browser exemption deliberately applies only to
/// `wasm32-unknown-unknown`. It does not use `target_family = "wasm"`, which
/// would incorrectly exempt threaded targets such as `wasm32-wasip1-threads`.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait MaybeSync: Sync {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Sync> MaybeSync for T {}

/// A portability bound that does not require [`Sync`] in browsers.
///
/// IndexedDB handles can contain `JsValue`s, which are not `Sync`, so browser
/// storage implementations need this marker to accept every type. Native and
/// threaded WebAssembly targets still receive the `Sync` supertrait; in
/// particular, the predicate excludes `wasm32-wasip1-threads`. Defining that
/// difference only here preserves the native storage contract.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait MaybeSync {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T> MaybeSync for T {}
