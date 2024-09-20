# Rust Install

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
echo 'export PATH=$HOME/.cargo/bin:$PATH' | tee -a .bashrc
source .bashrc
rustup --version
cargo --version
```

# Rust Run

```bash
cargo run
```

or

```bash
cargo build
./target/debug/orderbook
```