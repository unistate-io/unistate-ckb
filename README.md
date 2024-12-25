# Unistate-CKB

Unistate-CKB is a Rust-based project designed to index and manage various components of the CKB blockchain, including inscriptions, XUDT cells, spores, and clusters. This project leverages modern Rust features and libraries to ensure high performance and reliability.

## Features

- **Inscription Indexing**: Efficiently indexes inscription information and associates it with relevant blockchain data.
- **XUDT Cell Support**: Handles XUDT (XCLAIM Universal Data Type) cells, including type scripts and dependencies.
- **Spore Management**: Manages spores with enhanced type checking and dependency handling.
- **Cluster Handling**: Supports clusters by adding and maintaining type IDs.
- **Database Optimization**: Utilizes advanced database upsert functions and conflict resolution strategies.
- **Parallel Processing**: Implements parallel processing for improved performance during indexing.
- **Dependency Height Finder**: Adds a new feature to find block heights of specific cell dependencies.
- **Subcommands**: Supports `run` and `fetch-dep-heights` subcommands for flexible usage.

## Getting Started

### Prerequisites

- Rust (stable)
- PostgreSQL (or other supported databases)

### Installation

#### From Source

1. Clone the repository:

    ```sh
    git clone https://github.com/unistate-io/unistate-ckb.git
    cd unistate-ckb
    ```

2. Build the project:

    ```sh
    cargo build --release
    ```

3. Run the application:

    ```sh
    ./target/release/unistate-ckb run --apply-init-height true
    ```

### Configuration

The configuration file `unistate.toml` should be located in the root directory of the project. You can customize the database connection settings, network parameters, and other configurations as needed.

### Usage

To see the available commands and options:

```sh
./target/release/unistate-ckb --help
```

#### Example Commands

- **Run the indexer**:

    ```sh
    ./target/release/unistate-ckb run --apply-init-height true
    ```

- **Fetch and print dependency heights**:

    ```sh
    ./target/release/unistate-ckb fetch-dep-heights
    ```

## Contributing

Contributions are welcome! Please read our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to the project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
