/// A `Future` that is `Send`.
macro_rules! future_send {
    ($t:ty) => {
        impl ::core::future::Future<Output = $t> + Send
    };
}

pub(crate) use future_send;
