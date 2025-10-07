#!/bin/bash
# Quick Start Script for Stock Price History Downloader

set -e  # Exit on error

echo "=========================================="
echo "Stock Price History Downloader"
echo "Quick Start Setup"
echo "=========================================="
echo ""

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python $python_version found"
echo ""

# Check if pip is available
echo "Checking pip..."
if command -v pip3 &> /dev/null; then
    echo "✓ pip3 found"
else
    echo "✗ pip3 not found. Please install pip."
    exit 1
fi
echo ""

# Install dependencies
echo "Installing dependencies..."
echo "This may take a minute..."
pip3 install -q -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Verify installation
echo "Verifying installation..."
python3 -c "from app import StockDataDownloader; print('✓ Application imported successfully')"
echo ""

# Run simple test
echo "=========================================="
echo "Running simple test..."
echo "=========================================="
echo "This will download 1 month of price data for AAPL"
echo "Please wait..."
echo ""

python3 simple_test.py

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next Steps:"
echo "1. Review the data in ./data/price-history/AAPL/"
echo "2. Check the log file: price_history_downloader.log"
echo "3. Read USER_GUIDE.md for detailed usage"
echo "4. Read DEPLOYMENT_GUIDE.md for Snowflake deployment"
echo ""
echo "Example commands:"
echo "  python3 example_usage.py    # See more examples"
echo "  python3 app.py              # Run full test suite"
echo ""
echo "Happy downloading! 🚀"

