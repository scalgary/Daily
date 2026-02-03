#!/bin/bash

# Get the project root (parent of .devcontainer)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# Check if already initialized
if [ ! -f pyproject.toml ]; then
    uv init --no-readme
fi

# Add all dependencies including nbstripout
uv add jupyter ipykernel ruff pylint pyspark pandas polars
uv add --dev nbstripout

echo "✅ uv.lock created successfully"

# Create .gitattributes
echo '*.ipynb filter=nbstripout' > .gitattributes

# Commit changes
git add pyproject.toml uv.lock .gitattributes
git commit -m "Update uv dependencies and configure nbstripout"

echo "🔄 Now rebuild manually: Cmd+Shift+P → 'Dev Containers: Rebuild Container'"

echo "🔄 Now reload windows: Cmd+Shift+P → 'Developer: Reload Window'"

