import pandas as pd
import numpy as np

df = pd.read_excel('bank_transactions.xlsx', sheet_name='Sheet1', engine='openpyxl')
json_data = df.to_json(orient="records", force_ascii=False)
with open('bank_transactions.json', 'w', encoding="utf-8") as f:
    f.write(json_data)
    
# pip install lxml
xml_data = df.to_xml(root_name="Transactions", row_name="Transaction", index=False)
with open('bank_transactions.xml', "w", encoding="utf-8") as x:
    x.write(xml_data)