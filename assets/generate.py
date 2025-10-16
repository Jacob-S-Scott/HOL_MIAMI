#!/usr/bin/env python3
"""
QR Code Generator
Generates a QR code from a URL specified in .env file
"""

import os
import qrcode
from dotenv import load_dotenv
from pathlib import Path


def generate_qr_code():
    """Generate a QR code from the URL specified in .env file"""

    # Load environment variables from .env file
    load_dotenv()

    # Get the target URL from environment variable
    target_url = os.getenv("TARGET_URL")

    if not target_url:
        raise ValueError("TARGET_URL not found in .env file")

    print(f"Generating QR code for: {target_url}")

    # Create QR code instance
    qr = qrcode.QRCode(
        version=1,  # Controls the size of the QR code
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )

    # Add data to the QR code
    qr.add_data(target_url)
    qr.make(fit=True)

    # Create an image from the QR code
    img = qr.make_image(fill_color="black", back_color="white")

    # Save the image
    output_path = Path(__file__).parent / "qrcode.jpg"
    img.save(output_path)

    print(f"✓ QR code successfully generated: {output_path}")
    print(f"  Scan this code to open: {target_url}")


if __name__ == "__main__":
    try:
        generate_qr_code()
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

