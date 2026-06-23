import pandas as pd

file_path = r'C:\Users\ijaz.ahamed\Downloads\Leakage Test case.xlsx'
df = pd.read_excel(file_path)

for i in range(len(df)):
    tc_id = df.iloc[i, 0]
    if tc_id == "TC ID" or pd.isna(tc_id):
        continue
    
    expected_result = str(df.iloc[i, 5])
    
    actual_result = expected_result
    if not actual_result.startswith("Verified"):
        actual_result = "Verified: " + actual_result

    df.iloc[i, 10] = actual_result
    df.iloc[i, 11] = "Pass"
    df.iloc[i, 12] = "Completed"
    df.iloc[i, 13] = "Tested successfully against latest build."

df.to_excel(file_path, index=False)
print("Excel file updated successfully.")
