#!/usr/bin/env python3
"""
Test script to validate UI functionality without running Streamlit server
"""

import sys
import pandas as pd
from datetime import datetime

def test_ui_imports():
    """Test that all UI components can be imported"""
    try:
        from ui import (
            get_stock_manager,
            normalize_price_data_schema,
            validate_price_data_schema,
            render_price_metrics,
            render_price_chart,
            COLORS,
            POPULAR_TICKERS
        )
        print("✓ All UI imports successful")
        return True
    except Exception as e:
        print(f"✗ Import error: {e}")
        return False

def test_schema_validation():
    """Test schema validation with various data formats"""
    from ui import normalize_price_data_schema, validate_price_data_schema
    
    # Test with old Snowflake format
    old_sf_data = pd.DataFrame({
        'TICKER': ['AAPL', 'AAPL'],
        'DATE': ['2023-01-01', '2023-01-02'],
        'OPEN_PRICE': [150.0, 151.0],
        'HIGH_PRICE': [155.0, 156.0],
        'LOW_PRICE': [149.0, 150.0],
        'CLOSE_PRICE': [154.0, 155.0],
        'VOLUME': [1000000, 1100000]
    })
    
    # Test with new format
    new_format_data = pd.DataFrame({
        'TICKER': ['AAPL', 'AAPL'],
        'DATE': ['2023-01-01', '2023-01-02'],
        'OPEN': [150.0, 151.0],
        'HIGH': [155.0, 156.0],
        'LOW': [149.0, 150.0],
        'CLOSE': [154.0, 155.0],
        'VOLUME': [1000000, 1100000]
    })
    
    # Test normalization
    old_normalized = normalize_price_data_schema(old_sf_data)
    new_normalized = normalize_price_data_schema(new_format_data)
    
    # Test validation
    old_valid = validate_price_data_schema(old_normalized)
    new_valid = validate_price_data_schema(new_normalized)
    
    if old_valid and new_valid:
        print("✓ Schema validation working for both formats")
        return True
    else:
        print(f"✗ Schema validation failed: old={old_valid}, new={new_valid}")
        return False

def test_data_access():
    """Test that we can access data without errors"""
    from ui import get_stock_manager, normalize_price_data_schema
    
    try:
        manager = get_stock_manager()
        print("✓ Stock manager initialized")
        
        # Test local data access
        tickers = manager.get_available_tickers_local()
        print(f"✓ Found {len(tickers)} local tickers: {tickers[:3] if tickers else 'none'}")
        
        # Test with sample ticker if available
        if tickers:
            ticker = tickers[0]
            df = manager.get_existing_price_data(ticker)
            if df is not None and not df.empty:
                df_norm = normalize_price_data_schema(df)
                print(f"✓ Successfully normalized data for {ticker}: {df_norm.shape}")
            else:
                print(f"ℹ No local data for {ticker}")
        
        return True
    except Exception as e:
        print(f"✗ Data access error: {e}")
        return False

def main():
    """Run all tests"""
    print("="*60)
    print("TESTING UI FUNCTIONALITY")
    print("="*60)
    
    tests = [
        ("UI Imports", test_ui_imports),
        ("Schema Validation", test_schema_validation),
        ("Data Access", test_data_access)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append(False)
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for i, (test_name, _) in enumerate(tests):
        status = "✓ PASS" if results[i] else "✗ FAIL"
        print(f"{test_name}: {status}")
    
    all_passed = all(results)
    print(f"\nOverall: {'✓ ALL TESTS PASSED' if all_passed else '✗ SOME TESTS FAILED'}")
    
    if all_passed:
        print("\n🚀 UI is ready to launch with: streamlit run ui.py")
    else:
        print("\n⚠️  Please fix the failing tests before launching the UI")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
