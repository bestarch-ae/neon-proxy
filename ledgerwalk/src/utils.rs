macro_rules! ward {
    (@log [] $str:literal, $($arg:tt)*) => {
        $crate::utils::ward!(@log [warn] $str, $($arg)*)
    };
    (@log [warn] $str:literal, $($arg:tt)*) => {
        ::tracing::warn!($($arg)* $str)
    };
    (@log [error] $str:literal, $($arg:tt)*) => {
        ::tracing::error!($($arg)* $str)
    };
    (@log [$tag:ident] $_:literal, $($__:tt)*) => {
        compile_error!(concat!("unsupported log level: ", stringify!($tag)));
    };

    ($result:expr) => {
        $crate::utils::ward!([] $result, "")
    };
    ([$level:ident] $result:expr) => {
        $crate::utils::ward!([$level] $result, "")
    };
    ($result:expr, $str:literal) => {
        $crate::utils::ward!([] $result, $str)
    };
    // ([$level:ident] $result:expr, $str:literal) => {
    //     $crate::utils::ward!([$level] $result, $str)
    // };
    ([$($level:ident)?] $result:expr, $str:literal $(, $args:tt)* $(,)?) => {
        match $result {
            Ok(value) => value,
            Err(err) => {
                $crate::utils::ward!(@log [$($level)?] $str, error = %err, $($args)*);
                return;
            }
        }
    };

}

pub(crate) use ward;
