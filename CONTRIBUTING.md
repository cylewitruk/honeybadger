# Contributing

## Code Conventions

- Prefer string interpolation for `format_args!`-style macro invocations (e.g. `println!("{my_var}")` over `println!("{}", my_var)`). Interpolation can of course not be used for nested expressions (i.e. `my_obj.my_var`), in which case you should use the appropriate placerholders, preferably named (e.g. `println!("{my_var}", my_var = my_obj.my_var)`).
- Prefer `Result<T, E>` over panick-able constructs (except in test code). If panick-able constructs are used (i.e. if checks performed and the code truly cannot panic), include a `SAFETY:` comment explaining why it cannot panic. It is generally preferable to not return a `Result` for simpler methods if the safety **can indeed be guaranteed**.
