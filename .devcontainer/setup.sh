#!/bin/bash

# Initialize uv project
uv init --no-readme

# Add all dependencies
uv add jupyter ipykernel ruff pylint pyspark pandas polars

echo "✅ uv.lock created successfully"

# Commit changes
git add pyproject.toml uv.lock
git commit -m "Update uv dependencies"

echo "🔄 Now rebuild manually: Cmd+Shift+P → 'Dev Containers: Rebuild Container'"

echo "🔄 Now reload windows: Cmd+Shift+P → 'Developer: Reload Window"'"

