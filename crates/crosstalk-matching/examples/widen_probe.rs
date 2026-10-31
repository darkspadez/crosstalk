use crosstalk_matching::pyre::{pythonize, widen_case_insensitive_i};
fn main() {
    let p = r"\(([KW][A-Z]{3})(?:-[A-Z\s]+)?\)";
    println!("pythonize: {}", pythonize(p));
    println!("widened  : {}", widen_case_insensitive_i(&pythonize(p)));
}
