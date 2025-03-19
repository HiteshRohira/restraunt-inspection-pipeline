#!/bin/bash

# Exit on any error
set -e

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Create project directories
echo "Creating project directories..."
mkdir -p raw_data processed_data output_data

# Create virtual environment
echo "Creating virtual environment..."
python3.11 -m venv venv || handle_error "Failed to create virtual environment. Please make sure python3.11 and venv are installed."

# Check if virtual environment was created successfully
if [ ! -f "venv/bin/activate" ]; then
    handle_error "Virtual environment creation failed. venv/bin/activate not found."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate || handle_error "Failed to activate virtual environment"

# Verify virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    handle_error "Virtual environment is not activated properly"
fi

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt || handle_error "Failed to install dependencies"

echo "Setup completed successfully!"
echo "Remember to activate the virtual environment with 'source venv/bin/activate' before running any scripts"