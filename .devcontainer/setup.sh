#!/bin/bash

echo "🚀 Starting devcontainer setup..."
echo "📍 Current directory: $(pwd)"
echo "👤 Current user: $(whoami)"

# Initialize uv project if needed
if [ ! -f "uv.lock" ]; then
    echo "📦 No uv.lock found - initializing new project..."
    
    echo "   ⚙️  Running uv init..."
    uv init --no-readme
    
    echo "   📋 Adding main dependencies..."
    uv add selenium webdriver-manager python-dotenv pillow
    
    echo "   📊 Adding data science packages..."
    uv add jupyter ipykernel matplotlib pandas numpy
    
    echo "   🔧 Adding dev dependencies..."
    uv add --dev pytest ruff mypy
    
    echo "✅ New project initialized successfully!"
else
    echo "📦 Found uv.lock - syncing existing dependencies..."
    uv sync
    echo "✅ Dependencies synced successfully!"
fi

echo "🎉 Devcontainer setup completed!"
echo "💡 You can now use:"
echo "   - uv run python script.py"
echo "   - uv run jupyter notebook"
echo "   - uv run pytest"
echo "   - uv run ruff check"