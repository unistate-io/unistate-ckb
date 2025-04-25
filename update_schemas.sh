#!/bin/bash

# Directory containing the .mol schema files
SCHEMAS_DIR="schemas"

# Directory where the generated Rust files should be written
OUTPUT_DIR="src/schemas"

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Process each .mol file in the schemas directory
for mol_file in "$SCHEMAS_DIR"/*.mol; do
    # Get the base filename without extension
    base_name=$(basename "$mol_file" .mol)

    # The corresponding Rust file path
    rust_file="$OUTPUT_DIR/$base_name.rs"

    echo "Generating $rust_file from $mol_file..."

    # Run moleculec and redirect output to the Rust file
    moleculec --schema-file "$mol_file" --language rust > "$rust_file"

    # Check if the command succeeded
    if [ $? -ne 0 ]; then
        echo "Error generating $rust_file"
        exit 1
    fi

    echo "Successfully generated $rust_file"
done

echo "All schema files processed successfully."

# Format the generated Rust files
echo "Formatting generated Rust files..."
cargo fmt

# Remove 'ref ' from generated Rust files
echo "Removing 'ref ' from generated Rust files..."
for rust_file in "$OUTPUT_DIR"/*.rs; do
    sed -i '' 's/ref //g' "$rust_file"
done

# Format the generated Rust files
echo "Formatting generated Rust files..."
cargo fmt
