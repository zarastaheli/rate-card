# Bug Fixes and Debugging

## Issues Fixed

### 1. Redundant Import Statements
**Problem**: The `re` module was imported multiple times inside functions even though it was already imported at the top.

**Fix**: Removed redundant `import re` statements from:
- `normalize_service_name()` function
- Zip code extraction in `generate_rate_card()` (two locations)

**Impact**: Cleaner code, no functional change.

### 2. Zone Column Handling
**Problem**: The zone column name was being saved to config AFTER the config was already written to disk, so it wasn't available during Excel generation.

**Fix**: 
- Moved zone column detection to happen BEFORE saving the config
- Store `zone_column` in the config dictionary before writing it to disk
- This ensures the zone column name is available during Excel generation

**Impact**: Zone-based invoices now correctly write zone values to Excel.

### 3. Formula Preservation Logic
**Problem**: The formula copying logic used `ws.max_row` which could include empty template rows, potentially copying formulas to wrong rows or missing data rows.

**Fix**: 
- Calculate `last_data_row` based on the actual number of data rows written
- Only copy formulas to rows where we actually wrote data (from `start_row` to `last_data_row`)

**Impact**: Formulas are now correctly preserved only in rows with actual data.

### 4. Error Handling in Generation
**Problem**: The `generate_rate_card()` function could fail silently or with unclear errors.

**Fix**: 
- Added try-catch block around `generate_rate_card()` call in the `/api/generate` endpoint
- Added validation for template file existence
- Added validation for 'Raw Data' sheet existence
- Better error messages returned to client

**Impact**: Better error reporting when generation fails.

### 5. Config Saving Order
**Problem**: Config was being saved before zone column was detected, causing the zone column name to be missing.

**Fix**: Reordered operations:
1. Load raw CSV
2. Detect zone column
3. Create config with zone column
4. Save config
5. Create normalized data

**Impact**: Zone column is now properly saved and available for Excel generation.

## Testing Recommendations

After these fixes, test the following scenarios:

1. **Zone-based invoice**: Upload a CSV with a zone column, verify zone values are written to Excel
2. **Zip-based invoice**: Upload a CSV without zone column, verify origin zip is written
3. **Formula preservation**: Generate a rate card and verify formula columns (24-29) still contain formulas
4. **Error handling**: Try generating with missing template file to verify error message
5. **Service matching**: Verify QUALIFIED column is correctly set based on selected services

## Files Modified

- `app.py`: Fixed imports, zone column handling, formula preservation, error handling

