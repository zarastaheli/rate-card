/**
 * Office Script for Rate Card Dashboard Metrics
 * 
 * HOW TO USE:
 * 1. Open your Rate Card template in Excel Online (OneDrive/SharePoint)
 * 2. Go to Automate tab → New Script
 * 3. Paste this entire script
 * 4. Save the script (name it "GetDashboardMetrics")
 * 5. Create a Power Automate flow that calls this script
 */

function main(
  workbook: ExcelScript.Workbook,
  payload: {
    toggles?: { address: string; value: string | number | boolean }[];
    outputs: string[];
  }
): Record<string, string | number | boolean | null> {
  
  // Apply any toggles (carrier selection changes)
  if (payload.toggles && payload.toggles.length > 0) {
    for (const t of payload.toggles) {
      try {
        workbook.getRange(t.address).setValue(t.value);
      } catch (e) {
        console.log(`Failed to set ${t.address}: ${e}`);
      }
    }
  }

  // Force full recalculation
  workbook.getApplication().calculate(ExcelScript.CalculationType.full);

  // Read output values
  const result: Record<string, string | number | boolean | null> = {};
  for (const addr of payload.outputs) {
    try {
      const value = workbook.getRange(addr).getValue();
      result[addr] = value;
    } catch (e) {
      result[addr] = null;
      console.log(`Failed to read ${addr}: ${e}`);
    }
  }

  return result;
}

/**
 * POWER AUTOMATE FLOW SETUP:
 * 
 * 1. Go to https://make.powerautomate.com
 * 2. Create → Instant cloud flow
 * 3. Trigger: "When an HTTP request is received"
 * 4. Add action: "Excel Online (Business)" → "Run script"
 *    - Location: OneDrive for Business (or SharePoint)
 *    - Document Library: OneDrive (or your SharePoint library)
 *    - File: Select your Rate Card workbook
 *    - Script: Select "GetDashboardMetrics"
 *    - ScriptParameters/payload: Use expression: triggerBody()
 * 5. Add action: "Response"
 *    - Status Code: 200
 *    - Body: Use the "result" output from the "Run script" action
 * 6. Save the flow
 * 7. Copy the HTTP POST URL from the trigger (this goes in Replit secrets)
 * 
 * EXAMPLE REQUEST BODY (what Replit sends):
 * {
 *   "toggles": [
 *     {"address": "'Pricing & Summary'!F5", "value": "Yes"},
 *     {"address": "'Pricing & Summary'!F6", "value": "No"}
 *   ],
 *   "outputs": [
 *     "'Pricing & Summary'!C5",
 *     "'Pricing & Summary'!C6", 
 *     "'Pricing & Summary'!C7",
 *     "'Pricing & Summary'!C11",
 *     "'Pricing & Summary'!C12"
 *   ]
 * }
 * 
 * EXAMPLE RESPONSE:
 * {
 *   "'Pricing & Summary'!C5": 11454.81,
 *   "'Pricing & Summary'!C6": 0,
 *   "'Pricing & Summary'!C7": 11454.81,
 *   "'Pricing & Summary'!C11": 0.456,
 *   "'Pricing & Summary'!C12": 0.456
 * }
 */
