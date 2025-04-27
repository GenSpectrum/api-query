#[macro_export]
macro_rules! clone {
    { } => {
    };
    { $var:ident } => {
        let $var = $var.clone();
    };
    { $var:ident, $($rest:tt)* } => {
        let $var = $var.clone();
        $crate::clone!{$($rest)*}
    }
}
