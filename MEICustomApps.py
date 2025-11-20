import hashlib
import pymssql
import datetime
from datetime import date, timedelta
import logging
import time
import calendar
import requests
import json
from tabulate import tabulate
import os
import getpass
import hashlib
import csv
import pandas as pd
from colorama import init, Fore, Style
from decimal import Decimal

init(autoreset=True)  # biar warna otomatis reset

# print(Fore.RED + "Teks Merah")
# print(Fore.GREEN + "Teks Hijau")
# print(Fore.YELLOW + "Teks Kuning")
# print(Fore.BLUE + "Teks Biru")
# print(Style.BRIGHT + Fore.LIGHTGREEN_EX + "Teks LIGHTGREEN_EX Tebal")


# pyinstaller --onefile --noconsole contoh.py

server = '127.0.0.1'  #'127.0.0.1' #'103.232.32.196-Live' #'196.168.168.96-VNC' # 
server = '103.232.32.196'  #'127.0.0.1' #'103.232.32.196-Live' #'196.168.168.96-VNC' # 
port = 1433
user = 'useresb'
password = 'mitoeasybuser201612'
database = 'dummy_easyb' #'dummy_easyb' # 'dummy_easyb_live' # 'dummy_easyb_vnc' #
database = 'MITO2024' #'MITO2024' # 'MAJU' # 'dummy_easyb_vnc' #

##### REAMRRKKKKS:
# update createProductTrannsfer function to set CreateBy and EditBy to UserLogin include GDV
# 2025-11-06 - add set location destination of PTRP if interlocation transfer
# 2025-11-07 - add condition user branch to create transaction
# 2025-11-07-2 - add condition where warehourse id to create transaction
# 2025-11-11 - add update product to iseller function
# 2025-11-19 - add create product iseller

global Version
Version = "251119"

print(f'Connect to {database[:3]}...')
conn = pymssql.connect(server=server, port=port, user=user, password=password, database=database,autocommit=False)

UserLogin = ""
BranchIDLogin = 0
AutoGDV = False
Apps = f"##### MEI Custom Apps v.{Version} #####"

def load_menu(selected=None):
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    cond = ""
    if not selected:
        cond = " and code <=9 "
    else:
        cond = f" and code like '{selected}%' "
    cursor = conn.cursor()
    sql_menu = f""" 
        select code, menu from tmp_menu_master where active = 1 {cond} order by code
    """
    # print(sql_menu)
    cursor.execute(sql_menu)
    rows = cursor.fetchall()
    cursor.close()
    clear()
    print(f"{Apps}\n")
    print("Select Menu:")
    idx = 0
    for row in rows:
        idx += 1
        code = row[0]
        menu_name = row[1]
        if idx == 1 and selected:
            print(f"{menu_name}")
        else:
            print(f"[{code}] {menu_name}")
    if not rows:
        print(Fore.RED +"❌ Menu not found.\n")
    print("[0] Exit\n")

    # print(tabulate(rows, headers=["Code", "Menu Name", "Active"], tablefmt="github"))
    # print(f"Total rows: {len(rows)}\n\n")
    pass

def cekVersion():
    print(f"{Apps}\n")
    print("Checking application version...")
    cursor = conn.cursor()
    sql_version = f""" 
        select ver from tmp_iseller_token where id = 1 and ver = '{Version}'
    """
    cursor.execute(sql_version)
    row = cursor.fetchone()
    cursor.close()
    if row:
        version = row[0]
        print(f"Application Version: {version} has been uptodate.")
        time.sleep(1)
        clear()
    else:
        print(Fore.RED +"❌ Application has been outdated.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to exit...")
        clear()
        exit()
    return True

def main():
    # load_menu()
    cekVersion()
    while True:
        print(f"{Apps}\n")
        print("Select Menu:")
        print("[1] Transaction")
        print("[2] Master Data")
        print("[3] Report")
        print("[0] Exit\n")

        menu = input(Fore.LIGHTGREEN_EX +"Select the menu [ ] and press enter: ")

        if menu == '1':
            clear()
            # load_menu('1')
            print("[1] Transaction")
            print("    [11] Import PO to EasyB")
            print("    [12] Import Product Promo Special Price to Iseller")
            print("    [13] Import WHT TRPT ONLY")
            print("    [14] Import WHT Auto GDV Warehouse Transfer")
            print("Select the transaction and press enter: ")
            cmd21_query = input(Fore.LIGHTGREEN_EX +"Select Transaction: ")
            if cmd21_query == '11':
                cek_user()
                importPO2()
            elif cmd21_query == '12':
                cek_user()
                createPromoNonTrigger()
                return
            elif cmd21_query == '13':
                cek_user()
                createProductTrannsfer()
                return
            elif cmd21_query == '14':
                cek_user()
                global AutoGDV 
                AutoGDV = True
                createProductTrannsfer()
                return
            else:
                print(Fore.RED +"❌ Please Select the menu.\n")
                # load_menu()
                continue
        elif menu == '2':
            clear()
            # load_menu('2')
            print("[2] Master Data")
            print("    [20] ALL Product Trenly")
            print("    [21] Lookup by SKU contains")
            # print("    [22] Lookup by SKU contains")
            # print("    [23] Lookup by Product Name starts with")
            print("    [24] Lookup by Product Name contains")
            print("    [25] Create New Product")
            print("    [26] Update Product iSeller")
            print("    [27] Lookup Supplier/Vendor by Name contains")
            print("    [28] Create Supplier/Vendor")
            cmd21_query = input(Fore.LIGHTGREEN_EX +"Select Lookup by: ")
            sql_Query = "SELECT ProductID, Field1 SKU, ProductName FROM MsProduct where 1=1 and flagactive=1 and CreateBy in ('usermaju','import','purchase') and ProductID >= 77471 "
            if cmd21_query == '20':
                masterProduct(sql_Query + " order by productName")
                
            elif cmd21_query == '21x':
                cmd21_input = input(Fore.LIGHTGREEN_EX +"Lookup by SKU starts with: ")
                if not cmd21_input:
                    print(Fore.RED +"❌ Please Select the menu.\n")
                    # load_menu()
                    continue
                sql_Query = f"{sql_Query} and Field1 LIKE '{cmd21_input}%%' order by Field1"
                masterProduct(sql_Query)
            elif cmd21_query == '21':
                cmd21_input = input(Fore.LIGHTGREEN_EX +"Lookup by SKU contains: ")
                if not cmd21_input:
                    print(Fore.RED +"❌ Please Select the menu.\n")
                    # load_menu()
                    continue
                sql_Query = f"{sql_Query} and Field1 LIKE '%%{cmd21_input}%%' order by Field1"
                masterProduct(sql_Query)
            elif cmd21_query == '23x':
                cmd21_input = input(Fore.LIGHTGREEN_EX +"Lookup by Product Name starts with: ")
                if not cmd21_input:
                    print(Fore.RED +"❌ Please Select the menu.\n")
                    # load_menu()
                    continue
                sql_Query = f"{sql_Query} and ProductName LIKE '{cmd21_input}%%' order by ProductName"
                masterProduct(sql_Query)
            elif cmd21_query == '24':
                cmd21_input = input(Fore.LIGHTGREEN_EX +"Lookup by Product Name contains: ")
                if not cmd21_input:
                    print(Fore.RED +"❌ Please Select the menu.\n")
                    # load_menu()
                    continue
                sql_Query = f"{sql_Query} and ProductName LIKE '%%{cmd21_input}%%' order by ProductName"
                masterProduct(sql_Query)
            elif cmd21_query == '25':
                print("Create New Product")
                print("    [250] Import New Product from Excel File")
                print("    [251] Create New Product Manual")
                print("    [252] Create New Product iSeller Only")
                cmd25_query = input(Fore.LIGHTGREEN_EX +"Select Create New Product by: ")
                if cmd25_query == '250':
                    cek_user()
                    createNewProduct(import_mode='excel')
                elif cmd25_query == '251':
                    cek_user()
                    createNewProduct(import_mode=None)
                elif cmd25_query == '252':
                    cek_user()
                    excel = input(Fore.LIGHTGREEN_EX +"Create by Excel or Manual? press y for Excel, n for Manual (y/n): ").lower()
                    if excel == 'y':
                        createNewProductIseller(import_mode='excel')
                    else:
                        createNewProductIseller(import_mode=None)
                else:
                    print(Fore.RED +"❌ Please Select the menu.\n")
                    # load_menu()
                    continue
            elif cmd21_query == '26':
                updateProductIseller()
            elif cmd21_query == '27':
                cmd21_input = input(Fore.LIGHTGREEN_EX +"Lookup by Supplier Name contains: ")
                sql_Query = f""" select supplierID 'Supplier ID', storeName 'Supplier Name' from MsSupplier where 1=1 and storename like '%{cmd21_input}%' order by storeName """
                masterProduct(sql_Query)
            elif cmd21_query == '28':
                cek_user()
                createVendor()
            else:
                print(Fore.RED +"❌ Menu not found. Please select the menu above.\n\n")

        # elif menu == '22':
        #     print("[221] Lookup by Branch Name starts with")
        #     print("[222] Lookup by Branch Name contains")
        #     where = ""
        #     cmd22_query = input(Fore.LIGHTGREEN_EX +"Select Lookup by: ")
        #     if cmd22_query == '221':
        #         cmd22_input = input(Fore.LIGHTGREEN_EX +"Lookup by Branch Name starts with: ")
        #         sql_Query = f"BranchName LIKE '{cmd22_input}%%' order by BranchName"
        #         masterProduct(sql_Query)
        #     elif cmd22_query == '222':
        #         cmd22_input = input(Fore.LIGHTGREEN_EX +"Lookup by Branch Name contains: ")
        #         sql_Query = f"BranchName LIKE '%%{cmd22_input}%%' order by BranchName"
        #         masterProduct(sql_Query)
        #     else:
        #         print(Fore.RED +"❌ Menu not found. Please select 221 or 222.\n\n")
        #         load_menu()

        elif menu == '3':
            print("[30] Report Stock ALL Trenly")
            print("[31] Report Pending Product Transfer")
            opt = input(Fore.LIGHTGREEN_EX +"Select Report: ")
            if opt == '30':
                cek_user()
                reportStockAll()
            elif opt == '31':
                cek_user()
                reportPendingProductTransfer()
            else:
                print(Fore.RED +"❌ Menu not found. Please select 30.\n\n")
                # load_menu()
                # continue
        elif menu == 'su':
            cek_user()
            if not UserLogin in ('mitospv','USERMAJU'):
                print(Fore.RED +"❌ You are not authorized to access this menu.\n\n")
                input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
                clear()
                main()

            print("[1] Refesh iSeller Token")
            input_menu = input(Fore.LIGHTGREEN_EX +"Select Menu: ")
            menu = input_menu.strip()
            if menu == '1':
                refreshIsellerToken()
            else:
                return False
        elif menu == '0':
            exit()
        else:
            print(Fore.RED +"❌ Menu not found. Please select 1, 2, 3.\n\n")
            # load_menu()
            # clear()

def masterProduct(sql_Query):
    # print("Implement your function logic here transactionPO")
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    # else:
    #     print(f'Koneksi Datebase {database} Berhasil!!!!!!!!')  

    if not sql_Query:
        pass
    
    cursor = conn.cursor()
    qeury =f"""{sql_Query} """
    cursor.execute(qeury)
    columns = [col[0] for col in cursor.description]

    # ambil data
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        print(Fore.RED +"❌ Data not found.\n")
        input(Fore.LIGHTGREEN_EX +"Search again? y/n: ").lower()
        # if input().lower() == 'y':
        clear()

            
        return True
    print(tabulate(rows, headers=columns, tablefmt="github"))
    print(f"Total rows: {len(rows)}\n\n")
    # jadikan DataFrame
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        print(Fore.RED +"❌ Data not found.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        clear()
        return True
    quest = input(Fore.LIGHTGREEN_EX +"Are you want to export the query results to a XLSX file? (y/n): ").lower()
    if quest != 'y':
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        clear()
        return True
    # simpan ke XLSX
    filename = input(Fore.LIGHTGREEN_EX +"Enter the output XLSX file name: ")
    if not filename.endswith('.xlsx'):
        filename += '.xlsx'
    df.to_excel(f"Export/{filename}", index=False)
    print(f"Data has been exported to Folder Export/{filename}")
    input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
    clear()
    main()
    # 
    # print(qeury)

def importPO2():
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    else:
        print(f'Connecting to Database {database[:3]} successful!')  
    
    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True

    print("Import Purchase Order to EasyB from Excel File")
    fname = ""
    fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
    if not fname.endswith('.xlsx'):
        fname += '.xlsx'
    file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
    if not os.path.exists(file_path):
        print(Fore.RED +f"❌ File {fname} not found, Please check the file in the import folder.\n")
        main()
        return True
    # else:
    #     print(f'File {fname} found, processing...')
    try:
        df = pd.read_excel(file_path, sheet_name=0, header=None)

        # ambil header manual
        date_value = df.iloc[0, 1] if pd.notna(df.iloc[0, 1]) else ""
        vendor_name = df.iloc[1, 1] if pd.notna(df.iloc[1, 1]) else ""
        branch_name = df.iloc[2, 1] if pd.notna(df.iloc[2, 1]) else ""
        ppn_value = df.iloc[3, 1] if pd.notna(df.iloc[3, 1]) else 0
        currency = df.iloc[4, 1] if pd.notna(df.iloc[4, 1]) else ""

        # Produk mulai baris ke-6
        products = df.iloc[6:, :5]
        products.columns = ["Sku", "Productname", "Qty", "Price", "Total"]

        # ambil unique SKU dari file
        file_skus = products["Sku"].dropna().astype(str).unique().tolist()

        # --- 2. Query sekali ke DB ---
        cursor = conn.cursor()
        queryWhere = "and field1 = ''"
        if file_skus:
            queryWhere = "and field1 in %s" % str(tuple(file_skus)).replace(",)", ")")  # handle single item tuple

        query = f"""
        SELECT field1 Sku FROM MsProduct
        WHERE 1=1 {queryWhere}
        """
        cursor.execute(query)
        db_skus = [str(row[0]) for row in cursor.fetchall()]

        # --- 3. Bandingkan ---
        missing_skus = set(file_skus) - set(db_skus)
        products.index = products.index + 1 # agar index mulai dari 1 bukan 0 jadi sesuai row excel
        # cek duplikat
        duplicate_skus = products[products.duplicated(subset=["Sku"], keep=False)]
        # --- 4. Hasil validasi ---
        if missing_skus:
            print(Fore.RED +"❌ SKU not found in database:")
            for idx, row in products.iterrows():
                if str(row["Sku"]) in missing_skus:
                    print(Fore.RED +f"  - SKU {row['Sku']} (Row {idx})")
        elif not duplicate_skus.empty:
            print(Fore.RED +f"❌ Duplicate SKUs found in the file {fname}, please check products below.")
            print(duplicate_skus[["Sku", "Productname"]])
            print("")
        else:
            print("✅ All SKUs are valid and no duplicates found. Proceeding with import...")
            sure = input(Fore.LIGHTGREEN_EX +"Are you sure to continue import PO? (y/n): ").lower()
            if sure != 'y':
                print("Import cancelled by user.")
                return True
            Rate = 1
            if currency != 'IDR':
                sql_currency_rate = f"""SELECT TOP 1 coalesce(Rate,0) as Rate FROM MsCurrency WHERE Currency = '{currency}' and DateCur <= '{date_value}'  order by datecur desc"""
                # print(f"sql_currency_rate: {sql_currency_rate}")
                cursor = conn.cursor()
                cursor.execute(sql_currency_rate)
                Rate = cursor.fetchone()
                Rate = Rate[0] if Rate else 0.0 
                if not Rate or Rate == 0.0:
                    print(Fore.RED +f"❌ Rate for currency {currency} on {date_value} not found.\n")
                    cursor.close()
                    # return True
                print(f"Currency: {currency}, Rate: {Rate} on {date_value} or following latest date.")

            sql_check_branch = f"""SELECT BranchID, BranchName FROM MsBranch WHERE BranchName = '{branch_name}'"""
            cursor.execute(sql_check_branch)
            BranchID, BranchName = cursor.fetchone()
            if not BranchID:
                print(Fore.RED +f"❌ Branch '{branch_name}' not found in database.\n")
                cursor.close()
                return True
            print(f"Branch: {BranchName} (ID: {BranchID}) found in database.")

            sql_check_vendor = f"""SELECT SupplierID, StoreName FROM MsSupplier WHERE StoreName = '{vendor_name}'"""
            cursor.execute(sql_check_vendor)
            VendorID, VendorName = cursor.fetchone()
            if not VendorID:
                print(Fore.RED +f"❌ Vendor '{vendor_name}' not found in database.\n")
                cursor.close()
                return True
            print(f"Vendor: {VendorName} (ID: {VendorID}) found in database.")

            GrandTotal = 0.0
            sumqty = 0.0
            sumtotal = 0.0
            values = []
            for idx in range(6, len(df)):
                sql_product = f"""SELECT ProductID, ProductName FROM MsProduct WHERE Field1 = '{df.iloc[idx, 0]}'"""
                cursor.execute(sql_product)
                ProductID, ProductName = cursor.fetchone()
                if not ProductID:
                    print(Fore.RED +f"❌ Product with SKU '{df.iloc[idx, 0]}' not found in database.\n")
                    cursor.close()
                    return True
                
                Qty = float(df.iloc[idx, 2])
                Price = float(df.iloc[idx, 3])
                TotalPrice = Price * Qty
                ppn_amt = TotalPrice * (ppn_value / 100) if ppn_value else 0
                # TotalPrice += ppn_amt
                GrandTotal += (TotalPrice + ppn_amt)
                sumqty += Qty
                sumtotal += TotalPrice
                values.append(
                    f"('TransNum', -1, {ProductID}, 1, {Qty}, {Price}, 0, {ppn_value},{TotalPrice}, '{date_value}', '-', 1)"
                )
                print(f"Line {idx+1}: {ProductID} - {df.iloc[idx, 0]} - {ProductName} - Qty: {Qty} - Price: {Price} - TotalPrice: {TotalPrice}")

            values = ', '.join(values) + ";"
            dateFormat = date_value.strftime("%y%m")
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 1 TransNum, SUBSTRING(TransNum, 5, 100) AS seq
                FROM POHead
                WHERE TransNum LIKE %s
                ORDER BY TransNum DESC
            """, f'TRPO{dateFormat}%')
            last_po = cursor.fetchone()
            next_po_number = f"TRPO{int(last_po[1]) + 1:06d}" if last_po else f"TRPO{dateFormat}0000001"

            values = values.replace("TransNum", next_po_number)  # Escape single quotes for SQL
            pohead_sql = f"""
                INSERT INTO POHead (
                    TransNum, TDate, [Type], BOMNum, SupplierID, Currency, Rate, BranchID,
                    LocationID, ChargesTotal, GrandTotal, EstForwarderTotal, EstImportTotal,
                    ForwarderTotal, ImportTotal, PaymentMethod, AdditionalInfo,
                    AuthorizationNotes, POName, POApproval, Status, DivisionLevel1ID,
                    DivisionLevel2ID, DivisionLevel3ID, DivisionLevel4ID, CreateBy,
                    CreateDate, EditBy, EditDate
                ) VALUES (
                    '{next_po_number}', '{date_value}', 0, '', {VendorID}, '{currency}', {Rate}, {BranchID}, -1, 0, {GrandTotal}, 0, 0, NULL, NULL, '2',
                    '{UserLogin}', NULL, '{UserLogin}', '{UserLogin}', '4', NULL, NULL, NULL, NULL, '{UserLogin}',
                    GETDATE(), NULL, NULL)
                """
            cursor.execute(pohead_sql)
            # print(pohead_sql)

            sql_podetail = f"""
                INSERT INTO PODetail (
                    TransNum, PRDetailID, ProductID, UOMID, Qty, Price, Discount, PPN,
                    TotalPrice, DateNeeded, Notes, Status
                ) VALUES
                    {values}
                """
            cursor.execute(sql_podetail)

            sql_podetail_update = f"""
                UPDATE PODetail set TotalPrice = (Qty * Price) + (Qty * Price * PPN / 100) where TransNum = '{next_po_number}'
            """
            cursor.execute(sql_podetail_update)

            # print(sql_podetail)
            conn.commit()
            cursor.close()
            print(f"✅ Successfully imported PO {next_po_number} with GrandTotal: {GrandTotal}, Qty: {sumqty} of {idx - 5} items.\n")
            # input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
            # clear()

    except Exception as e:
        print(Fore.RED +f"❌ Error reading Excel file {fname}: {e}\n")
        main()
        return True

# def importPO():
#     if not conn:
#         print("Unable to connect to EasyB server.")
#         exit()
#     else:
#         print(f'Connecting to Database {database[:3]} successful!')  
    
#     if not UserLogin:
#         print(Fore.RED +"❌ User not logged in.\n")
#         main()
#         return True
#     fname = ""
#     fname = input(Fore.LIGHTGREEN_EX +"Enter the csv file name with semicolons(;)separated: ")
#     # fname = 'pomm0021.csv' if not fname else fname.strip()
#     file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
#     try:
#         with open(file_path, 'r', encoding="latin1", newline="") as file:
#             reader = csv.DictReader(file, delimiter=";", quotechar='"')
#             idx = 0
#             GrandTotal = 0
#             values = []
#             Rate = 1.0
#             sumqty = 0.0
#             sumtotal = 0.0
#             for lline in reader:
#                 if not lline:  # skip empty row
#                     continue

#                 # if idx == 0:  # skip header
#                 #     idx += 1
#                 #     continue

#                 Rate = 1
#                 if lline['Currency'] != 'IDR':
#                     sql_currency_rate = f"""SELECT TOP 1 coalesce(Rate,0) as Rate FROM MsCurrency WHERE Currency = '{lline['Currency']}' and DateCur <= '{lline['Date']}'  order by datecur desc"""
#                     # print(f"sql_currency_rate: {sql_currency_rate}")
#                     cursor = conn.cursor()
#                     cursor.execute(sql_currency_rate)
#                     Rate = cursor.fetchone()
#                     Rate = Rate[0] if Rate else 0.0 
#                     if not Rate or Rate == 0.0:
#                         print(Fore.RED +"❌ Rate for currency {lline[11]} on {lline[0]} not found.\n")
#                         cursor.close()
#                         return True
                    
#                 TDate = lline['Date']
#                 BranchID = lline['BranchID']
#                 BranchName = lline['BranchName']
#                 VendorID = lline['VendorID']
#                 VendorName = lline['VendorName']
#                 ProductID = lline['Productid']
#                 ProductName = lline['Productname']
#                 Qty = float(lline['Qty'])
#                 Price = float(lline['Price'].replace(',', '.'))
#                 Ppn = float(lline['Ppn'])
#                 Currency = lline['Currency']

#                 TotalPrice = Price * Qty
#                 GrandTotal += TotalPrice
#                 sumqty += Qty
#                 sumtotal += TotalPrice

#                 if Ppn:
#                     GrandTotal = GrandTotal * 1.11

#                 values.append(
#                     f"('TransNum', -1, {ProductID}, 1, {Qty}, {Price}, 0, {Ppn},{TotalPrice}, '{TDate}', '-', 1)"
#                 )
#                 idx += 1
#                 print(f"Line {idx}: {ProductID} - {ProductName} - Qty: {Qty} - Price: {Price} - TotalPrice: {TotalPrice}")

#             values = ', '.join(values) + ";"
#             dateFormat = datetime.datetime.now().strftime("%y%m")
#             cursor = conn.cursor()
#             cursor.execute("""
#                 SELECT TOP 1 TransNum, SUBSTRING(TransNum, 5, 100) AS seq
#                 FROM POHead
#                 WHERE TransNum LIKE %s
#                 ORDER BY TransNum DESC
#             """, f'TRPO{dateFormat}%')
#             last_po = cursor.fetchone()
#             next_po_number = f"TRPO{int(last_po[1]) + 1:06d}" if last_po else f"TRPO{dateFormat}0000001"

#             values = values.replace("TransNum", next_po_number)  # Escape single quotes for SQL
#             # print(f"values: {values}")
#             pohead_sql = f"""
#                 INSERT INTO POHead (
#                     TransNum, TDate, [Type], BOMNum, SupplierID, Currency, Rate, BranchID,
#                     LocationID, ChargesTotal, GrandTotal, EstForwarderTotal, EstImportTotal,
#                     ForwarderTotal, ImportTotal, PaymentMethod, AdditionalInfo,
#                     AuthorizationNotes, POName, POApproval, Status, DivisionLevel1ID,
#                     DivisionLevel2ID, DivisionLevel3ID, DivisionLevel4ID, CreateBy,
#                     CreateDate, EditBy, EditDate
#                 ) VALUES (
#                     '{next_po_number}', '{TDate}', 0, '', {VendorID}, '{Currency}', {Rate}, {BranchID}, -1, 0, {GrandTotal}, 0, 0, NULL, NULL, '2',
#                     '{UserLogin}', NULL, '{UserLogin}', '{UserLogin}', '4', NULL, NULL, NULL, NULL, '{UserLogin}',
#                     GETDATE(), NULL, NULL)
#                 """
#             # cursor.execute(pohead_sql)
#             # print(pohead_sql)

#             sql_podetail = f"""
#                 INSERT INTO PODetail (
#                     TransNum, PRDetailID, ProductID, UOMID, Qty, Price, Discount, PPN,
#                     TotalPrice, DateNeeded, Notes, Status
#                 ) VALUES
#                     {values}
#                 """
#             # cursor.execute(sql_podetail)
#             # print(sql_podetail)
#             conn.commit()
#             cursor.close()
#             print(f"✅ Successfully imported PO {next_po_number} with GrandTotal: {GrandTotal}, Qty: {sumqty} of {idx} items.\n")
#             input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
#             clear()

#     except FileNotFoundError:
#         print(Fore.RED +"❌ File {fname} not found, Please check the file in the import folder.\n")
#         main()

# def importPO_xlsx():
#     if not conn:
#         print("Unable to connect to EasyB server.")
#         exit()
#     else:
#         print(f'Connecting to Database {database[:3]} successful!')  

#     print("Import PO to EasyB")
#     fname = ""
#     fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
#     file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
#     if not os.path.exists(file_path):
#         print(Fore.RED +f"❌ File {fname} not found, Please check the file in the import folder.\n")
#         main()
#         return True
#     # read by default 1st sheet of an excel file
#     # file_path = "data.xlsx"

#     # Baca file, pastikan kolom Date jadi datetime
#     df = pd.read_excel(file_path, sheet_name=0, header=None)

#     # Pastikan numeric kolom Price dan Total
#     # df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
#     # df["Total"] = pd.to_numeric(df["Total"], errors="coerce")

#     # Perulangan tiap baris
#     values = []
#     GrandTotal = 0.0
#     sumqty = 0.0
#     sumtotal = 0.0
#     Rate = 1.0 
#     idx = 0
#     for row in range(len(df)):
#     # for idx, row in df.iterrows():
#         TDate = row["Date"].date()   # ambil hanya tanggal
#         BranchID = row["BranchID"]
#         branch_name = row["BranchName"]
#         VendorID = row["VendorID"]
#         vendor_name = row["VendorName"]
#         ProductID = row["Productid"]
#         ProductName = row["Productname"]
#         Qty = row["Qty"]
#         Price = row["Price"]
#         Ppn = row["Ppn"]
#         Total = row["Total"]
#         Currency = row["Currency"]

#         TotalPrice = Price * Qty
#         GrandTotal += TotalPrice
#         sumqty += Qty
#         sumtotal += TotalPrice

#         if Ppn:
#             GrandTotal = GrandTotal * 1.11

#         values.append(
#             f"('TransNum', -1, {ProductID}, 1, {Qty}, {Price}, 0, {Ppn},{TotalPrice}, '{TDate}', '-', 1)"
#         )

#         idx += 1
#         print(f"Line {idx}: {ProductID} - {ProductName} - Qty: {Qty} - Price: {Price} - TotalPrice: {TotalPrice}")
#     values = ', '.join(values) + ";"
#     dateFormat = TDate.strftime("%y%m")
#     cursor = conn.cursor()
#     cursor.execute("""
#         SELECT TOP 1 TransNum, SUBSTRING(TransNum, 5, 100) AS seq
#         FROM POHead
#         WHERE TransNum LIKE %s
#         ORDER BY TransNum DESC
#     """, f'TRPO{dateFormat}%')
#     last_po = cursor.fetchone()
#     next_po_number = f"TRPO{int(last_po[1]) + 1:06d}" if last_po else f"TRPO{dateFormat}0000001"

#     values = values.replace("TransNum", next_po_number)  # Escape single quotes for SQL
#     # print(f"values: {values}")
#     pohead_sql = f"""
#         INSERT INTO POHead (
#             TransNum, TDate, [Type], BOMNum, SupplierID, Currency, Rate, BranchID,
#             LocationID, ChargesTotal, GrandTotal, EstForwarderTotal, EstImportTotal,
#             ForwarderTotal, ImportTotal, PaymentMethod, AdditionalInfo,
#             AuthorizationNotes, POName, POApproval, Status, DivisionLevel1ID,
#             DivisionLevel2ID, DivisionLevel3ID, DivisionLevel4ID, CreateBy,
#             CreateDate, EditBy, EditDate
#         ) VALUES (
#             '{next_po_number}', '{TDate}', 0, '', {VendorID}, '{Currency}', {Rate}, {BranchID}, -1, 0, {GrandTotal}, 0, 0, NULL, NULL, '2',
#             '{UserLogin}', NULL, '{UserLogin}', '{UserLogin}', '4', NULL, NULL, NULL, NULL, '{UserLogin}',
#             GETDATE(), NULL, NULL)
#         """
#     # cursor.execute(pohead_sql)
#     # print(pohead_sql)

#     sql_podetail = f"""
#         INSERT INTO PODetail (
#             TransNum, PRDetailID, ProductID, UOMID, Qty, Price, Discount, PPN,
#             TotalPrice, DateNeeded, Notes, Status
#         ) VALUES
#             {values}
#         """
#     # cursor.execute(sql_podetail)

def createNewProduct(import_mode=None):
    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()

    sql_user_branch = f"""select top 1 BranchID from MsUser where UserName = '{UserLogin}'"""
    cursor = conn.cursor()
    cursor.execute(sql_user_branch)
    rows = cursor.fetchone()
    if not rows:
        print(Fore.RED +f"❌ User '{UserLogin}' not found in database.\n")
        main()
        return True
    BranchIDLogin = int(rows[0])
    if BranchIDLogin ==  1 or UserLogin.lower() == "usermaju" or UserLogin.lower() == "mitospv":
        pass
    else:
        print(Fore.RED +f"❌ User '{UserLogin}' not authorized to create new product.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to exit...")
        clear()
        main()

    if not import_mode:
        print("Create New Product")
        ProductName = input(Fore.LIGHTGREEN_EX +"Enter the Product Name: ").upper()
        Sku = input(Fore.LIGHTGREEN_EX +"Enter the SKU/Barcode: ").upper()
        price = input(Fore.LIGHTGREEN_EX +"Enter the Price (default 0): ")
        Category2 = input(Fore.LIGHTGREEN_EX +"Select one of the Category (Cashback, Point, Import, Lokal, Tester, Other): ").upper()
        Category_iseller = input(Fore.LIGHTGREEN_EX +"Enter Product Category iSeller (Ex: MMI / IMPORT): ").upper()
        if not ProductName or not Sku or not Category2:
            print(Fore.RED +"❌ Product Name, SKU, and Category are required.\n")
            return True
        tmp_products = [{'ProductName': ProductName, 'Sku': Sku, 'price': price, 'Category2': Category2, 'Category_iseller': Category_iseller}]
    elif import_mode == 'excel':
        print("Import Product from Excel")
        fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
        if not fname.endswith('.xlsx'):
            fname += '.xlsx'
        file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
        if not os.path.exists(file_path):
            print(Fore.RED +f"❌ File {fname} not found, Please check the file in the import folder.\n")
            main()
            return True
        # read by default 1st sheet of an excel file
        dataframe1 = pd.read_excel(file_path, sheet_name=0, header=None)
        tmp_products = []
        
        for row in range(1,len(dataframe1)):
            # product_name = str(product_name).strip().replace("nan", "") if str(product_name) == "nan" else str(product_name).strip()
            Sku = str(dataframe1.iloc[row, 0]).strip().replace("nan", "") if str(dataframe1.iloc[row, 0]) == "nan" else str(dataframe1.iloc[row, 0]).strip()
            ProductName = str(dataframe1.iloc[row, 1]).strip().replace("nan", "") if str(dataframe1.iloc[row, 1]) == "nan" else str(dataframe1.iloc[row, 1]).strip()
            Price = str(dataframe1.iloc[row, 2]).strip().replace("nan", "") if str(dataframe1.iloc[row, 2]) == "nan" else str(dataframe1.iloc[row, 2]).strip()
            Category2 = str(dataframe1.iloc[row, 3]).strip().replace("nan", "") if str(dataframe1.iloc[row, 3]) == "nan" else str(dataframe1.iloc[row, 3]).strip()
            Category_iseller = str(dataframe1.iloc[row, 4]).strip().replace("nan", "") if str(dataframe1.iloc[row, 4]) == "nan" else str(dataframe1.iloc[row, 4]).strip()
            if not ProductName or not Sku:
                print(Fore.RED +f"❌ Product Name, SKU, and Category are required. Please check row {row+2} in the file.\n")
                continue
            tmp_products.append({'ProductName': ProductName.upper(), 'Sku': Sku.upper(), 'price': Price, 'Category2': Category2.upper(), 'Category_iseller': Category_iseller.upper()})
    if not tmp_products:
        print(Fore.RED +"❌ No products to process.\n")
        return True

    get_all_sku = ('0')
    get_all_sku = str(tuple([prod['Sku'] for prod in tmp_products])).replace(",)", ")")
    cursor = conn.cursor()
    sql_check_sku = f"""select field1 'SKU', ProductName from MsProduct where Field1 in {get_all_sku}"""
    cursor.execute(sql_check_sku)
    rows_data = cursor.fetchall()
    count_data = len(rows_data)
    if count_data > 0:
        columns = [col[0] for col in cursor.description]
        print(Fore.RED +f"❌ {count_data} SKU(s) already exist in the database. Please check the list above.\n")
        print(tabulate(rows_data, headers=columns, tablefmt="github"))
        print("")
    else:
        idx = 0
        products = []
        for prod in tmp_products:
            idx += 1
            ProductName = prod['ProductName']
            Sku = prod['Sku']
            Price = prod['price']
            Category2 = prod['Category2']
            Category_iseller = prod['Category_iseller']
            products.append({
                'sku': Sku, 
                'name': ProductName, 
                'product_type': Category_iseller,
                'barcode': Sku,
                'price': Price if Price else 0,
                'continue_selling_when_sold_out': True,
                'taxable': True,
                'track_inventory': True,
                'type': 'standard',
                'require_shipping': False,
                'need_scale': False,
                'unit_of_measurement': 'pcs',
                'is_active': False
                # 'buying_price': 4000,
                # 'shipping_weight': 0,
                # 'shipping_unit': 'g',
                })

            print(f"Processing Product: {ProductName}, SKU: {Sku}, Category: {Category2}")
            # Cek apakah SKU sudah ada

            cursor = conn.cursor()
            sql_search_category2 = f"""select coalesce(Category2ID,0) from MsCategory2 where Category2 like '%{Category2}'"""
            cursor.execute(sql_search_category2)
            Category2ID = cursor.fetchone()
            Category2ID = Category2ID[0] if Category2ID else 0

            if Category2ID == 0:
                print(Fore.RED +f"❌ Category '{Category2}' not found in database.\n")
                return True

            sql_create_main_product = f"""
                INSERT INTO MsCategory6 (Category6,Notes,FlagActive,CreateBy,CreateDate)
                OUTPUT INSERTED.Category6ID
                values ('{ProductName}', '', 1, '{UserLogin}', GETDATE())
            """
            cursor.execute(sql_create_main_product)
            Category6ID = cursor.fetchone()[0]

            sql_create_product = f"""
                INSERT INTO MsProduct (ProductName,CategoryID,Category2ID,Category3ID,Category4ID,Category5ID,SupplierID,ProductionCapacity,PackagingCapacity,MinQty,Notes,PPN,Inclusive,ProductType,Field1,Field2,Field3,Field4,Field5,Field6,Field7,Field8,ImageURL,FlagActive,CreateBy,CreateDate)
                output INSERTED.ProductID
                values ('{ProductName}', 1, {Category2ID}, -1, -1, -1, -1, 0, 0, 0, '', 0, 0, 0, '{Sku}', '', '', '', '', '', '', '{Category6ID}', '', 1, '{UserLogin}', GETDATE())
            """
            cursor.execute(sql_create_product)
            ProductID = cursor.fetchone()[0]
            barcodeNumber = "RW1-" + str(ProductID) +"-1"

            sql_create_product_detail = f"""
                INSERT INTO MsProductDetail (BarcodeNumber,ProductID,UOMID,Qty,BuyPrice,SellPrice,SellPriceBeforeTax)
                values ('{barcodeNumber}', {ProductID}, 1, 1, 0, 0, 0)
            """
            cursor.execute(sql_create_product_detail)
            
            conn.commit()
            if not ProductID:
                print(f"❌ Failed to create new product '{ProductName}' with SKU '{Sku}'.\n")

            print(f"✅ {idx}. Successfully created new product '{ProductName}' with SKU '{Sku}', ProductID: {ProductID}, BarcodeNumber: {barcodeNumber}.")
        print(f"\nTotal {idx} new products have been created successfully.\n")

    # cursor.close()
    iseller = input(Fore.LIGHTGREEN_EX +"Are you want to create iseller product now? (y/n): ").lower()
    if iseller != 'y':
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        main()

    sql_iseller_token = f"""SELECT Token FROM tmp_iseller_token WHERE id= 1 AND Token IS NOT NULL"""
    cursor = conn.cursor()  
    cursor.execute(sql_iseller_token)
    token = cursor.fetchone()
    token = token[0] if token else None
    cursor.close()

    payload = json.dumps({
        "products": products
    })
    print(payload)
    
    url = "https://trenly.isellershop.com//api/v3/CreateProducts"
    headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token,
            'Cookie': '.Stackify.Rum=cafedaed-35f5-4c2d-9712-8cb02bd538a4'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code in (495, 496, 525, 526):
        response = requests.request("POST", url, headers=headers, data=payload, verify=False)
    json_data = json.loads(response.text)
    if response.status_code == 200:
        # print(json_data)
        if json_data['error_message'] == None:
            prod_success = []
            prod_failed = []
            for prod in json_data['products']:
                if prod['error_message'] == None:
                    prod_success.append(f"✅ Product '{prod['product_id']}' with SKU '{prod['sku']}' created in iseller with ID {prod['product_id']}.")
                else:
                    prod_failed.append(f"❌ Failed to create product '{prod['product_id']}' with SKU '{prod['sku']}' in iseller. {prod['error_message']}")
            if prod_success:
                print(Fore.LIGHTGREEN_EX +"✅ Successfully created iseller products below.")
                print(Fore.LIGHTGREEN_EX +f" {prod_success}")
            if prod_failed:
                print(Fore.RED +f"❌ Failed to create iseller products below.")
                print(Fore.RED +f" {prod_failed}")
        else:
            print(Fore.RED +f"❌ Failed to create iseller product. {json_data['error_message']}")
    else:
        print(Fore.RED +f"❌ Failed to create iseller product. Status Code: {response.status_code}, Response: {response.text}")

    input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
    main()




    # "products": [{
    #     "sku": "GFSUSMYO0001",
    #     "name": "Sushi Mayo",
    #     "handle": "sushi-mayo",
    #     "barcode": "10640",
    #     "description": "Sushi Mayo",
    #     "type": "standard",
    #     "tags": ["sushi", "makanan"],
    #     "price":65000,
    #     "buying_price": 40000,
    #     "taxable": true,
    #     "track_inventory": true,
    #     "continue_selling_when_sold_out": true,
    #     "require_shipping": true,
    #     "shipping_weight": 250,
    #     "shipping_unit": "g",
    #     "unit_of_measurement": "portion",
    #     "variants": null,
    #     "composite": null,
    #     "comboset" : null,
    #     "need_scale" : false,
    #     "is_active": true
    # }]

def updateProduct():
    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    pass

def createVendor():
    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    
    vendor_name = input(Fore.LIGHTGREEN_EX +"Enter the Vendor Name: ").upper().strip()
    if not vendor_name:
        print(Fore.RED +"❌ Vendor Name is required.\n")
        return True
    
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    
    cursor = conn.cursor()
    sql_check_vendor = f"""SELECT SupplierID, StoreName FROM MsSupplier WHERE StoreName = '{vendor_name}'"""
    cursor.execute(sql_check_vendor)
    VendorID, VendorName = cursor.fetchone() or (None, None)
    if VendorID:
        print(Fore.RED +f"❌ Vendor '{VendorName}' already exists in database with ID {VendorID}.\n")
        cursor.close()
        return True
    sql_create_vendor = f"""
        INSERT INTO MsSupplier (StoreName,npwpno, Notes, FlagActive, CreateBy, CreateDate,pic,address,phone1,fax,email)
        output INSERTED.SupplierID
        values ('{vendor_name}', '111.111.111.1-111.111', '', 1, '{UserLogin}', GETDATE(), '', '', '', '', '')
        """
    cursor.execute(sql_create_vendor)
    VendorID = cursor.fetchone()[0]
    conn.commit()
    if not VendorID:
        print(Fore.RED +f"❌ Failed to create new vendor '{vendor_name}'.\n")
        cursor.close()
        return True
    print(Fore.LIGHTGREEN_EX +f"✅ Successfully created new vendor '{vendor_name}', VendorID: {VendorID}.\n")
    cursor.close()

def reportStockAll():
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    else:
        print(f'Connecting to Database {database[:3]} successful!')  

    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    
    sql_query = f"""
        SELECT	b.sku 'SKU', b.ProductName, f.branchName 'Branch', c.LocationName 'Location', 
        b.Category2 'ProductGroup', b.Category 'ProductType',
        SUM(a.InQty-a.OutQty)'Qty', (SUM((a.InQty*a.HPP)-(a.OutQty*a.HPP))) 'Value'
        FROM StockCard a
        LEFT JOIN (
        SELECT b.ProductID, d.Category2, e.Category, 
        case when f.Category6ID = b.Field8 then f.Category6 else b.ProductName end 'ProductName', b.field1 'SKU'
        FROM MsProduct b
        LEFT JOIN MsCategory2 d ON d.category2ID = b.category2ID
        LEFT JOIN MsCategory e ON e.CategoryID = b.CategoryID
        LEFT join MsCategory6 f on b.Field8 = f.Category6ID
        )b ON a.ProductID = b.ProductID
        JOIN MsLocation c ON c.LocationID = a.LocationID
        JOIN MsBranch f ON c.BranchID = f.BranchID
        JOIN MsProduct k ON b.ProductID = k.ProductID
        WHERE f.BranchName like '%trenly%'
        and c.LocationID in (select LocationID from MsLocation WHERE BranchID in (select BranchID from MsBranch mb WHERE BranchName like '%trenly%'))
        GROUP BY b.ProductName, f.branchName, c.LocationName, b.Category2, b.Category,b.sku
        HAVING SUM(a.InQty-a.OutQty) <> 0
        ORDER BY b.ProductName
        -- ORDER BY b.Category2 desc
        OPTION (USE HINT ('FORCE_DEFAULT_CARDINALITY_ESTIMATION'))
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    columns = [col[0] for col in cursor.description]

    # ambil data
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print(tabulate(rows, headers=columns, tablefmt="github"))
    print(f"Total rows: {len(rows)}\n\n")
    # jadikan DataFrame
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        print(Fore.RED +"❌ Data not found.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        clear()
        return True
    quest = input(Fore.LIGHTGREEN_EX +"Are you want to export the query results to a XLSX file? (y/n): ").lower()
    if quest != 'y':
        clear()
        return True
    
    # simpan ke XLSX
    filename = input(Fore.LIGHTGREEN_EX +"Enter the output XLSX file name: ")
    if not filename.endswith('.xlsx'):
        filename += '.xlsx'
    df.to_excel(f"Export/{filename}", index=False)
    print(f"Data has been exported to Folder Export/{filename}")

def reportPendingProductTransfer():
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    else:
        print(f'Connecting to Database {database[:3]} successful!')  

    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    
    sql_query = f"""
        select c.BranchName 'Branch Asal',a.TransNum,a.TDate,d.BranchName 'Branch Tujuan',b.ProductID,e.ProductName, b.Qty,b.Notes 
        from ProductTransferHead a 
        join ProductTransferDetail b on b.TransNum = a.TransNum
        join MsBranch c on c.BranchID = a.OriginBranch
        join MsBranch d on d.BranchID = a.DestinationBranch
        join MsProduct e on e.ProductID = b.ProductID
        where 1=1
        and a.[Status] <> 2
        and a.OriginBranch in (select BranchID from MsBranch where BranchName like 'Trenly%')
        and a.TransNum not in (select RefNum from GoodsDeliveryHead where 1=1 and TransType='Transfer Request' and BranchID in (select BranchID from MsBranch where BranchName like 'Trenly%'))
        order by c.BranchName, a.TDate
        OPTION (USE HINT ('FORCE_DEFAULT_CARDINALITY_ESTIMATION'))
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    columns = [col[0] for col in cursor.description]

    # ambil data
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print(tabulate(rows, headers=columns, tablefmt="github"))
    print(f"Total rows: {len(rows)}\n\n")
    # jadikan DataFrame
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        print(Fore.RED +"❌ Data not found.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        clear()
        return True
    quest = input(Fore.LIGHTGREEN_EX +"Are you want to export the query results to a XLSX file? (y/n): ").lower()
    if quest != 'y':
        clear()
        return True
    
    # simpan ke XLSX
    filename = input(Fore.LIGHTGREEN_EX +"Enter the output XLSX file name: ")
    if not filename.endswith('.xlsx'):
        filename += '.xlsx'
    df.to_excel(f"Export/{filename}", index=False)
    print(f"Data has been exported to Folder Export/{filename}")

def createNewProductIseller(import_mode=None):
    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    
    sql_iseller_token = f"""SELECT Token FROM tmp_iseller_token WHERE id= 1 AND Token IS NOT NULL"""
    cursor = conn.cursor()
    cursor.execute(sql_iseller_token)
    token = cursor.fetchone()
    token = token[0] if token else None
    if not token:
        print(Fore.RED +"❌ Iseller API token not found in database.\n")
        main()
        return True
    
    products = []
    if import_mode == 'excel':
        print("Create New Product iSeller from Excel")
        fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
        if not fname.endswith('.xlsx'):
            fname += '.xlsx'
        file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
        if not os.path.exists(file_path):
            print(Fore.RED +"❌ File not found.\n")
            return True  
        
        # read by default 1st sheet of an excel file
        dataframe1 = pd.read_excel(file_path, sheet_name=0, header=None)
        
        for data in dataframe1.values[1:]:
            print('===',data)

            item_dict = {}
            sku = data[0]
            product_name = data[1] if data[1] else None
            price = data[2] if data[2] else None
            category = data[2] if data[2] else None
            category2 = data[4] if data[4] else None

            sku = str(sku).strip().replace("nan", "") if str(sku) == "nan" else str(sku).strip()
            if not sku:
                print(Fore.RED +"❌ Product SKU is required.\n")
                input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
                main()
            
            item_dict.update({
                'sku': sku,
                'continue_selling_when_sold_out': True,
                'is_active': True
            })
            product_name = str(product_name).strip().replace("nan", "") if str(product_name) == "nan" else str(product_name).strip()
            if product_name:
                item_dict.update({'name': product_name})
            category2 = str(category2).strip().replace("nan", "") if str(category2) == "nan" else str(category2).strip()
            if category2:
                item_dict.update({'product_type': category2})
            price = str(price).strip().replace("nan", "") if str(price) == "nan" else str(price).strip()
            if price:
                item_dict.update({'price': price})
            products.append(item_dict)
        print("Prepare to create new product iSeller below: \n", products)

    else:
        while True:
            item_dict = {}
            print("Create New Product iSeller")
            sku = input(Fore.LIGHTGREEN_EX +"Enter the Product SKU: ").upper().strip()
            product_name = input(Fore.LIGHTGREEN_EX +"Enter the Product Name: ").upper().strip()
            price = input(Fore.LIGHTGREEN_EX +"Enter the Product Price: ").strip()
            category = input(Fore.LIGHTGREEN_EX +"Enter the Product Category: ").upper().strip()
            category2 = input(Fore.LIGHTGREEN_EX +"Enter the Detail Category: ").upper().strip()
            # products = [(sku, product_name, price, stock, category, description)]

            if not sku:
                print(Fore.RED +"❌ Product SKU is required.\n")
                again = input(Fore.LIGHTGREEN_EX +"Do you want to try again? (y/n): ").lower()
                if again == 'y':
                    continue
                else:
                    main()
            else:
                break

        item_dict.update({
            'sku': sku,
            'continue_selling_when_sold_out': True,
            'is_active': True
        })
        if product_name:
            item_dict.update({'name': product_name})
        if category2:
            item_dict.update({'product_type': category2})
        if price:
            item_dict.update({'price': price})
        products.append(item_dict)

    if not products:
        print(Fore.RED +"❌ No products to process.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        main()

    payload = {
        'products': products
    }
    print(json.dumps(payload, indent=4))

    # url = "https://trenly.isellershop.com//api/v3/CreateProducts"
    # headers = {
    #     'Content-Type': 'application/json',
    #     'Authorization': 'Bearer ' + token,
    #     'Cookie': '.Stackify.Rum=cafedaed-35f5-4c2d-9712-8cb02bd538a4'
    # }

def updateProductIseller():
    getToken = f"""SELECT Token FROM tmp_iseller_token WHERE id= 1 AND Token IS NOT NULL"""
    cursor = conn.cursor()
    cursor.execute(getToken)
    token = cursor.fetchone()
    token = token[0] if token else None
    if not token:
        print(Fore.RED +"❌ Iseller API token not found in database.\n")
        main()
        return 
    
    print("[26] Update Product iSeller")
    print("    [260] Update Products by Excel File")
    print("    [261] Update Product Manually")
    input_option = input(Fore.LIGHTGREEN_EX +"Select an option: ").strip()
    if input_option not in ['260', '261']:
        print(Fore.RED +"❌ Invalid option selected.\n")
        return True
    if input_option == '260':
        print("Update Products by Excel File")
        fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
        if not fname.endswith('.xlsx'):
            fname += '.xlsx'
        file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
        if not os.path.exists(file_path):
            print(Fore.RED +"❌ File not found.\n")
            return True 
        
        # read by default 1st sheet of an excel file
        products = []
        dataframe1 = pd.read_excel(file_path, sheet_name=0, header=None)
        
        for data in dataframe1.values[1:]:
            sku = data[0]
            product_name = data[1] if data[1] else None
            price = data[2]
            category = data[3] if data[3] else None
            category2 = data[4] if data[4] else None
    
            # sku = input(Fore.LIGHTGREEN_EX +"Enter the Product SKU to update in Iseller: ").upper().strip()
            if not sku:
                print(Fore.RED +"❌ Product SKU is required.\n")
                yorn = input(Fore.LIGHTGREEN_EX +"Do you want to try again? (y/n): ").lower()
                if yorn == 'y':
                    continue
                else:
                    clear()
                    break
            product_item = {
                "sku": sku,
                "continue_selling_when_sold_out": True,
                "is_active": True
            }
            product_name = str(product_name).strip().replace("nan", "") if str(product_name) == "nan" else str(product_name).strip()
            if product_name:
                product_item.update({'name': product_name})
            price = str(price).strip().replace("nan", "") if str(price) == "nan" else str(price).strip()
            if price:
                product_item.update({'price': float(price)})
            category2 = str(category2).strip().replace("nan", "") if str(category2) == "nan" else str(category2).strip()
            if category2:
                product_item.update({'product_type': category2})

            products.append(product_item)

    elif input_option == '261':
        clear()
        products = []
        while True:
            print("[261] Update Product Manually")
            sku = input(Fore.LIGHTGREEN_EX +"Enter the new Product SKU: ").upper().strip()
            product_name = input(Fore.LIGHTGREEN_EX +"Enter the new Product Name: ").strip()
            price = input(Fore.LIGHTGREEN_EX +"Enter the new Product Price: ").strip()

            if not sku:
                print(Fore.RED +"❌ Product SKU is required.\n")
                yorn = input(Fore.LIGHTGREEN_EX +"Do you want to try again? (y/n): ").lower()
                if yorn == 'y':
                    continue
                else:
                    clear()
                    break
            product_item = {
                "sku": sku,
                "continue_selling_when_sold_out": True,
                "is_active": True
            }
            if product_name:
                product_item.update({'name': product_name})
            if price:
                product_item.update({'price': price})
            break
        products.append(product_item)

    url = "https://trenly.isellershop.com//api/v3/UpdateProducts"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token,
    }
    payload = {
        "products": products
    }
    response = requests.request("POST", url, headers=headers, json=payload)
    if response.status_code in (495, 496, 525, 526):
        response = requests.request("POST", url, headers=headers, json=payload, verify=False)
    if response.status_code != 200:
        print(Fore.RED +"❌ Failed to update product.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        return
    
    json_data = json.loads(response.text)
    # json_data = {'products': [{'product_id': 'db1fbd7c-9c04-4e59-aa94-4150a882bc6b', 'product_header_id': 'b8efffd4-bc4e-4510-aa6c-5df03a732af5', 'sku': '8997017642151-TESTER', 'error_message': None, 'is_success': True}, {'product_id': '6fd2d298-cd68-4b1c-8e7a-f7bb8ba70f0c', 'product_header_id': '118c768b-1128-4254-b829-618d55dbe3ef', 'sku': '8997017642168-TESTER', 'error_message': None, 'is_success': True}], 'error_message': None, 'status': True, 'time': '00:00:00.2968739', 'error_detail': None}

    print(json_data)
    print(Fore.LIGHTGREEN_EX +"Product updated successfully.")
    input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
    clear()
    main()

        
def createPromoNonTrigger():
    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    
    sql_iseller_token = f"""SELECT Token FROM tmp_iseller_token WHERE id= 1 AND Token IS NOT NULL"""
    cursor = conn.cursor()  
    cursor.execute(sql_iseller_token)
    token = cursor.fetchone()
    token = token[0] if token else None
    if not token:
        print(Fore.RED +"❌ Iseller API token not found in database.\n")
        main()
        return True
    cursor.close()

    
    # token = "esqfjmnLq62qdva5mhvV7Sr-wYlNSEh8pCy_GhW34NwTE5P4WCgJclmK8PnEV9JY9VGSmv7YkoxqHC1s40_IC1Qme20aDwg9iau3oh48BQVf3gd79A7GZ5AhRPLqbkASt6OOrP5CZqV_WV93mWVX08BmUlUPJUkQN75-3twTq3P38Khf6wI7UrJUjuVAHGcbq6rhdgvJRmiggXPchQajgS_Bf12jIFooyufg_dAvuvYgc5dtHnx_kO-5fT8So_8P9rpPBnuGHSb8pr4xTk5m3TlAKrIH8MfAZO2lBaTJQXEqCktfdkY_SMiTx35q_t3g9Wjg88Rz7oZ1wXehmn9v3wYzzkYQN57kgwMf9V3M1NS_exHBl_3HdZEFiA60JxamTw-RlCVS35L5NKwMMzep5zGCarHi8CYfrrnPid_4O5a3IFDmKOJhCglgQrl9qp67GNcTcrdHPRdiuQalu4Foo3vTxfC3choXgSAx_qgiSwSD7fyI"

    headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token,
            'Cookie': '.Stackify.Rum=cafedaed-35f5-4c2d-9712-8cb02bd538a4'
    }
    url = "https://trenly.isellershop.com/api/v3/CreatePromotions"
    
    PromoProducts = []
    print("Import Product Promo Special Price to Iseller (Non Trigger)")
    fname = ""
    fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
    if not fname.endswith('.xlsx'):
        fname += '.xlsx'
    file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
    if not os.path.exists(file_path):
        print(Fore.RED +f"❌ File {fname} not found, Please check the file in the import folder.\n")
        main()
        return True
    
    # Ambil tahun dan bulan saat ini
    today = datetime.datetime.today()
    year = today.year
    month = today.month

    # Dapatkan jumlah hari dalam bulan tersebut
    priority = (calendar.monthrange(year, month)[1] + 1) - today.day

    # read by default 1st sheet of an excel file
    dataframe1 = pd.read_excel(file_path, sheet_name=0, header=None)
    PromoProducts = []
    Stores = []
    for row in range(len(dataframe1)):
        if row == 0:
            name =  dataframe1.iloc[row, 1] if pd.notna(dataframe1.iloc[row, 1]) else ""
        if row == 1:
            description = dataframe1.iloc[row, 1] if pd.notna(dataframe1.iloc[row, 1]) else ""
        if row == 2:
            Dates = dataframe1.iloc[row, 1]
            if pd.notna(Dates) and isinstance(Dates, str):
                DateParts = [d.strip() for d in Dates.split(",")]
                DateStart = datetime.datetime.strptime(DateParts[0], "%Y-%m-%d").strftime("%m/%d/%Y") if len(DateParts) > 0 else ""
                DateEnd = datetime.datetime.strptime(DateParts[1], "%Y-%m-%d").strftime("%m/%d/%Y") if len(DateParts) > 1 else ""
            else:
                DateStart = []
                DateEnd = []
        if row == 3:
            Stores = dataframe1.iloc[row, 1].strip("").replace(", ", ",").replace("  "," ").split(",") if pd.notna(dataframe1.iloc[row, 1]) else []
            Stores = Stores if Stores else []
        if row == 4:
            trigger = dataframe1.iloc[row, 1] if pd.notna(dataframe1.iloc[row, 1]) else ""
        if row == 5:
            flashsale_day = [d.strip() for d in dataframe1.iloc[row, 1].split(",")] if pd.notna(dataframe1.iloc[row, 1]) else ""
            old_day = ['senin','selasa','rabu','kamis','jumat','sabtu','minggu']
            new_day = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
            flashsale_day = [new_day[old_day.index(day)] for day in flashsale_day]
        if row == 6:
            flashtimes = [d.strip() for d in dataframe1.iloc[row, 1].split(",")] if pd.notna(dataframe1.iloc[row, 1]) else ""
            flashtimes_batch = []
            for time_range in flashtimes:
                time_parts = time_range.split("-")
                if len(time_parts) == 2:
                    start_time = time_parts[0].strip()
                    end_time = time_parts[1].strip()
                    flashtimes_batch.append({"start": start_time, "end": end_time})
        # if not flashtimes_batch:
        #     print(Fore.RED +f"❌ Flashsale times not found or invalid format in row 7.\n")
        #     print("Example format: 10:00-10:59, 13:00-13:59\n")
        #     print("Please check the file and try again.\n")
        #     input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        #     main()

        if row < 8:  # baris mulai data produk
            continue

            # ---- produk detail ----
        intital_promo = datetime.datetime.now().strftime("%m%d")
        sku = dataframe1.iloc[row, 0] if pd.notna(dataframe1.iloc[row, 0]) else None
        producName = dataframe1.iloc[row, 1] if pd.notna(dataframe1.iloc[row, 1]) else None
        qty = dataframe1.iloc[row, 2] if pd.notna(dataframe1.iloc[row, 2]) else None
        price = dataframe1.iloc[row, 3] if pd.notna(dataframe1.iloc[row, 3]) else None

        if sku:  # hanya append kalau ada SKU
            product_tmpl_iseller = {
                "promotion_title": intital_promo + "-" + str(sku),
                "description": name + " " + description,
                "condition": "product",
                "quantity": qty,
                "visibility": ["pos"],
                "outlet_visibility": Stores,
                "start_date": DateStart,  # format date "mm/dd/yyyy"
                "end_date": DateEnd, # format date "mm/dd/yyyy"
                "priority": priority,
                "allow_multiply": "true",
                # "is_active": "false"
            }
            if not trigger:
                product_tmpl_iseller.update({
                    "promotion_type": "product",
                    "item": sku,      # SKU produk syarat
                    "discount_type": "specific_amount",
                    "amount": price,
                })
            else:
                product_tmpl_iseller.update({
                    "item": trigger,      # SKU produk syarat
                    "promotion_type": "buy_get",
                    "quantity": 1,
                    "reward_type": "specific_amount",
                    "reward_source": "product",
                    "reward_quantity": 1,
                    "reward_sku": sku,  #// SKU reward
                    "reward_item": sku, #// nama produk persis
                    "amount": price,                   #// harga spesifik reward
                    "minimum_order": 1,
                })
            if flashtimes_batch:
                product_tmpl_iseller.update({
                    "valid_weekdays": flashsale_day, #["wednesday"],
                    "range_time": flashtimes_batch,
                    # [
                    #     {"start": "10:00", "end": "12:00"},
                    #     {"start": "13:00", "end": "14:00"},
                    #     {"start": "15:30", "end": "16:30"}
                    # ]
                })
            
            PromoProducts.append(product_tmpl_iseller)
    # print(PromoProducts)
    # adsf
    # setelah loop
    if not PromoProducts:
        print("No active promotions to push.")
    else:
        print(PromoProducts)
        # return
        # Promotions = json.dumps(Promotions)
        response = requests.request("POST", url, headers=headers, json=PromoProducts)
        if response.status_code in (495, 496, 525, 526):
            response = requests.request("POST", url, headers=headers, json=PromoProducts, verify=False)
        json_data = json.loads(response.text)
        # print(json_data)
        promotions = json_data.get("promotions", [])

        # ubah ke list untuk tabulate
        table = [
            [
                promo.get("promotion_id"),
                promo.get("promotion_title"),
                promo.get("is_active"),
                promo.get("error_message"),
                promo.get("is_success")
            ]
            for promo in promotions
        ]

        # header
        headers = ["Promotion ID", "Title", "Active", "Error Message", "Success"]

        #### {'promotions': [{'promotion_id': 'a6b7cd7b-8579-49b3-bf2f-aae00953c59e', 'promotion_title': 'PROMO MEDAN 30%', 'is_active': True, 'error_message': None, 'is_success': True}, {'promotion_id': '781f1ed8-72fe-47f4-bcd5-3c2ed09a756c', 'promotion_title': 'PROMO MEDAN 30%', 'is_active': True, 'error_message': None, 'is_success': True}], 'error_message': None, 'status': True, 'time': '00:00:00.1250026', 'error_detail': None}
        error_message = json_data.get('error_message')
        status = json_data.get('status')
        if status and not error_message:
            print(f"✅ {len(PromoProducts)} promos have been created successfully.\n")
        else:
            print(Fore.RED +"❌ "+ "Error, please check the error message below:\n")
            print(tabulate(table, headers=headers, tablefmt="grid"))

    input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
    clear()
    main()
    
def createProductTrannsfer():
    if not conn:
        print("Unable to connect to EasyB server.")
        exit()
    else:
        print(f'Connecting to Database {database[:3]} successful!')  

    if not UserLogin:
        print(Fore.RED +"❌ User not logged in.\n")
        main()
        return True
    menu = "Import WHT TRPT ONLY"
    if AutoGDV:
        menu = "Import WHT Auto GDV Warehouse Transfer"
    print(f"{menu}")
    fname = input(Fore.LIGHTGREEN_EX +"Enter the Excel file name: ")
    if not fname.endswith('.xlsx'):
        fname += '.xlsx'
    file_path = os.path.join("Import", fname)   # otomatis bikin path Import/fname
    if not os.path.exists(file_path):
        print(Fore.RED +f"❌ File {fname} not found, Please check the file in the import folder.\n")
        main()
        return True
    
    sql_user_branch = f"""select top 1 BranchID from MsUser where UserName = '{UserLogin}'"""
    cursor = conn.cursor()
    cursor.execute(sql_user_branch)
    rows = cursor.fetchone()
    if not rows:
        print(Fore.RED +f"❌ User '{UserLogin}' not found in database.\n")
        main()
        return True
    BranchIDLogin = int(rows[0])
    sql_where = ""
    if BranchIDLogin ==  1:
        sql_where = ""
    else:
        sql_where = f""" and b.BranchID = {BranchIDLogin} """

    # read by default 1st sheet of an excel file
    dataframe1 = pd.read_excel(file_path, sheet_name=0, header=None)
    dateValue = dataframe1.iloc[0, 1] 
    BranchSource = dataframe1.iloc[1, 1] 
    BranchDest = dataframe1.iloc[2, 1] 
    WhSource = dataframe1.iloc[3, 1] 
    WhDest = dataframe1.iloc[4, 1] 
    sql_branch = f"""select top 1 b.BranchID,b.BranchName,LocationID,LocationName from MsBranch b
                    join MsLocation l on l.BranchID = b.BranchID
                    WHERE 1=1
                    and b.BranchName = '{BranchSource}' and l.LocationName = '{WhSource}'
                    {sql_where}
                """
    cursor = conn.cursor()
    cursor.execute(sql_branch)
    rows = cursor.fetchone()
    if not rows:
        print(Fore.RED +f"❌ Branch '{BranchSource}' with Warehouse '{WhSource}' not found in database or you dont have access to it.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to Main Menu...")
        clear()
        main()
    BranchIDSource = rows[0]
    LocationIDSource = rows[2]

    sql_branch = f"""select top 1 b.BranchID,b.BranchName,LocationID,LocationName from MsBranch b
                    join MsLocation l on l.BranchID = b.BranchID
                    WHERE 1=1
                    and b.BranchName = '{BranchDest}'
                    and l.LocationName = '{WhDest}'
                """
    cursor = conn.cursor()
    cursor.execute(sql_branch)
    rows = cursor.fetchone()
    if not rows:
        print(Fore.RED +f"❌ Branch '{BranchDest}' with Warehouse '{WhDest}' not found in database.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to Main Menu...")
        clear()
        main()

    BranchIDDestination = rows[0] if rows else None
    LocationIDDestination = rows[2]

    sku_list = tuple(dataframe1.iloc[6:, 0].tolist())
    sku_tuple = str(tuple(sku_list)).replace(",)", ")")
    sql_product = f"""select field1 'SKU', ProductID from MsProduct where Field1 in {sku_tuple}"""
    cursor = conn.cursor()
    cursor.execute(sql_product)
    product_rows = cursor.fetchall()
    sku_exist  = [row[0] for row in product_rows]
    productIDs = {row[0]: row[1] for row in product_rows}

    sku_not_found = [item for item in sku_list if item not in sku_exist]

    if sku_not_found:
        print(Fore.RED +f"❌ The following SKUs were not found in the database:\n{', '.join(sku_not_found)}")
        print("Please check the file and try again.")
        input(Fore.LIGHTGREEN_EX +"Press Enter to Main Menu...")
        clear()
        main()
    

    start_row = 6
    batch_size = 300
    total_rows = len(dataframe1)
    while start_row < total_rows:
        end_row = min(start_row + batch_size, total_rows)

        yearMonth = datetime.datetime.now().strftime("%y%m")
        sequence_sql = """ 
            select top 1 right(TransNum,11) from ProductTransferHead pth WHERE TransNum LIKE '%s' order by TransNum desc
        """%('TRPT'+yearMonth+'%')
        
        cursor.execute(sequence_sql)
        # cursor.execute(sequence_sql, ('%' + yearMonth + '%',))
        result = cursor.fetchone()
        if result:
            next_number = int(result[0]) + 1
        else:
            next_number =  yearMonth + '0000001'
        TransNum = 'TRPT' + str(next_number)
        
        sku = dataframe1.iloc[start_row, 0]
        qty = dataframe1.iloc[start_row, 2]
        TransNum = TransNum
        TDate = dateValue
        DueDate = dateValue
        OriginBranch = BranchIDSource
        DestinationBranch = BranchIDDestination
        OriginLocation = LocationIDSource
        DestinationLocation = -1 if BranchSource != BranchDest else LocationIDDestination
        AdditionalInfo = UserLogin
        AuthorizationNotes = UserLogin
        Status = 3
        ProductTransferName = UserLogin
        ProductTransferApproval = UserLogin
        CreateBy = UserLogin
        CreateDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql_transfer_head = """
        INSERT INTO ProductTransferHead (TransNum,TDate,DueDate,OriginBranch,DestinationBranch,OriginLocation,DestinationLocation,AdditionalInfo,AuthorizationNotes,Status,ProductTransferName,ProductTransferApproval,CreateBy,CreateDate)
        VALUES ('%s', '%s', '%s', %s, %s, %s, %s, '%s', '%s', %s, '%s', '%s', '%s', '%s')
        """ %(TransNum, TDate, DueDate, OriginBranch, DestinationBranch, OriginLocation, DestinationLocation,AdditionalInfo, AuthorizationNotes, Status, ProductTransferName, ProductTransferApproval,CreateBy, CreateDate)
        cursor.execute(sql_transfer_head)

        # sql_transfer_detail = """
        # INSERT INTO ProductTransferDetail (TransNum,ProductID,UOMID,[Source],Destination,Qty,Notes,Status)
        # VALUES ('%s', %s, %s, %s, %s, %s, '%s', 1)
        # """ %(TransNum, line.x_product_id.easyb_product_id, 1,-1, -1, line.x_quantity, self.x_sequence)
        # cursor.execute(sql_transfer_detail)

        values = []
        product_ids = []
        for i in range(start_row, end_row):
            sku = dataframe1.iloc[i, 0]
            qty = dataframe1.iloc[i, 2]
            values.append((TransNum, int(productIDs.get(f"{sku}")), 1, -1, -1, qty, UserLogin, 1))
            product_ids.append(int(productIDs.get(f"{sku}")))
        values = str(values).replace("[", "").replace("]", "")

        sql_transfer_detail = f"""
        INSERT INTO ProductTransferDetail (TransNum,ProductID,UOMID,[Source],Destination,Qty,Notes,Status)
        VALUES {values}
        """
        cursor.execute(sql_transfer_detail)
        conn.commit()


        print(f"✅ Successfully created Product Transfer '{TransNum}' with {end_row - start_row} items.")
        if AutoGDV:
            print("Waiting for processing GDV...")

            sql_last_gdv = """
            SELECT TOP 1 RIGHT(TransNum, 4)
            FROM GoodsDeliveryHead WITH (UPDLOCK, ROWLOCK)
            WHERE TransNum LIKE %s
            ORDER BY TransNum DESC 
            """

            cursor.execute(sql_last_gdv, ('TRGDV' + yearMonth + '%',))
            row = cursor.fetchone()

            if row and row[0].isdigit():
                last_number = int(row[0])
                next_sequence = str(last_number + 1).zfill(4)
                next_gdv = 'TRGDV' + yearMonth + next_sequence
            else:
                next_gdv = 'TRGDV' + yearMonth + '0001'

            TransNumGDV = next_gdv
            RefNum = TransNum
            TransType = 'Transfer Request'
            TDate = dateValue.strftime("%Y-%m-%d %H:%M:%S")
            BranchID = BranchIDSource
            LocationID = LocationIDSource
            SenderID = 2
            ShippingFee = 0
            SubjectID = -1
            Driver = 'None'
            PoliceNumber = 'None'
            DeliveryInformation = 'None'
            AdditionalInfo = 'None'
            AuthorizationNotes = 'None'
            Status = 3  # Assuming 3 is the status for 'Done'
            GoodsDeliveryName = UserLogin
            GoodsDeliveryApproval = UserLogin
            CreateBy = UserLogin
            CreateDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            query_goods_delivery = """
            insert into GoodsDeliveryHead (TransNum, RefNum, TransType, TDate, BranchID, LocationID, SenderID, ShippingFee, SubjectID, Driver, PoliceNumber, DeliveryInformation, AdditionalInfo, AuthorizationNotes, Status, GoodsDeliveryName, GoodsDeliveryApproval, CreateBy, CreateDate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query_goods_delivery, (TransNumGDV, RefNum, TransType, TDate, BranchID, LocationID, SenderID, ShippingFee, SubjectID, Driver, PoliceNumber, DeliveryInformation, AdditionalInfo, AuthorizationNotes, Status, GoodsDeliveryName, GoodsDeliveryApproval, CreateBy, CreateDate))

            values_goods_delivery_detail = []
            for product in product_ids:
                print(f"Processing product ID: {product}", end="\r", flush=True)
                sql_get_podetail = """
                SELECT ID, ProductID, Qty FROM ProductTransferDetail WHERE TransNum = %s AND ProductID = %s
                """
                cursor.execute(sql_get_podetail, (TransNum, product))
                transferdetail_id = cursor.fetchone()
                # if not podetail:
                #     self.easyb_goods_delivery_no = "Error PODetail not found for product %s in transfer request %s." % (detail.product_id.name, transfer_request_no)
                #     raise UserError(_("No PODetail found for product %s in transfer request %s.") % (detail.product_id.name, transfer_request_no))
                
                sql_hpp = f"""
                    select StockDate, coalesce(qty,0)qty, coalesce(hpp,0)hpp from StockHpp 
                    where ProductID={product} and locationID={LocationID}
                    order by stockDate asc 
                """ 
                # print(sql_hpp)
                cursor.execute(sql_hpp)
                res_HPP = cursor.fetchall()
                if not res_HPP:
                    HPP = 999.12
                else:
                    tmp_qty = 0
                    tmp_hpp = 0
                    need_stock = int(transferdetail_id[2])  # 10
                    remain_qty = int(transferdetail_id[2])  # 3
                    for shpp in res_HPP:
                        if shpp[1] >= remain_qty and need_stock == remain_qty: # 10 >= 3 and 10 == 3
                            HPP = Decimal(shpp[2])
                            remain_qty = 0
                            # tmp_hpp += tmp_qty * Decimal(shpp[2])
                            # HPP = tmp_hpp / detail.qty_done if detail.qty_done > 0 else 999.12
                            break
                        
                        if int(shpp[1]) < int(remain_qty): # 7 < 10 remain_qty: # 7
                            remain_qty -= int(shpp[1])
                            tmp_hpp += int(shpp[1]) * Decimal(shpp[2]) # 7 * 1000 = 7000
                        else:
                            # tmp_qty = int(shpp[1]) - remain_qty # 10 - 3 = 7
                            tmp_hpp += int(remain_qty) * Decimal(shpp[2])
                            remain_qty = 0
                            HPP = tmp_hpp / int(transferdetail_id[2]) if int(transferdetail_id[2]) > 0 else 999.12
                            break
                    if remain_qty > 0:
                        HPP = 999.12

                values_goods_delivery_detail.append(f"('{TransNumGDV}', {transferdetail_id[0]}, {product}, 1, {transferdetail_id[2]}, 0, 0, 0, {HPP}, '', 1)")

            values_goods_delivery_detail_str = ", ".join(values_goods_delivery_detail)
            query_goods_delivery_detail = f"""
            INSERT INTO GoodsDeliveryDetail (TransNum,RefDetailID,ProductID,UOMID,Qty,ReceiveQty,ReturnQty,LostQty,HPP,Notes,Status)
            VALUES {values_goods_delivery_detail_str}
            """
            cursor.execute(query_goods_delivery_detail)
            try:
                print("waiting to approve GDV...")
                # time.sleep(20)
                cursor.execute("EXEC [dbo].[sp_ApproveGoodsDelivery] @TransNum = %s", (TransNumGDV,))
                conn.commit()
                print(f"Processed {end_row - start_row} records for GDV: {TransNumGDV}")
                # time.sleep(20)
            except:
                print(f"Error while executing sp_ApproveGoodsDelivery for GDV: {TransNumGDV}")

        start_row += batch_size
    print("All batches processed successfully.")
    input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
    clear()
    main()



        # Ambil SKU dari kolom 0 untuk batch ini
        # batch_sku_list = dataframe1.iloc[start_row:end_row, 0].astype(str).tolist()

        # print(f"Processing rows {start_row} to {end_row-1} — Total: {len(batch_sku_list)} SKU")


    # for row in range(6,len(dataframe1)):
    #     sku = dataframe1.iloc[row, 0]

def refreshIsellerToken():
    sql_token = f"""SELECT client_id,client_secret,callback_url,refresh_token,token FROM tmp_iseller_token WHERE id= 1"""
    cursor = conn.cursor()
    cursor.execute(sql_token)
    rec = cursor.fetchone()
    if not rec:
        print(Fore.RED +"❌ Iseller API credentials not found in database.\n")
        input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
        main()
        return
    
    old_token = rec[4]
    url = "https://isellershop.com/oauth/token"
    data = {
            'grant_type': 'refresh_token',
            'refresh_token': rec[3],
            'redirect_uri': rec[2],
            'client_id': rec[0],
            'client_secret': rec[1],
        }
    print(data)

    token = False
    refresh_token = False
    response = requests.request("POST", url, data=data)
    token = json.loads(response.text).get('access_token')
    refresh_token = json.loads(response.text).get('refresh_token')

    sql_update_token = f"""UPDATE tmp_iseller_token SET Token= %s, Refresh_Token = %s WHERE id= 1"""
    cursor.execute(sql_update_token, (token, refresh_token))
    conn.commit()
    cursor.close()

    print(Fore.LIGHTGREEN_EX +f"Old Token: {old_token}")
    print(Fore.LIGHTGREEN_EX +f"Token: {token}")
    print(Fore.LIGHTGREEN_EX +f"Refresh Token: {refresh_token}")
    input(Fore.LIGHTGREEN_EX +"Press Enter to continue...")
    main()


def cek_user():
    clear()
    global UserLogin
    global BranchIDLogin
    global AutoGDV
    UserLogin = ""

    input_username = input(Fore.LIGHTGREEN_EX +"User Name: ").strip()
    # input_password = input(Fore.LIGHTGREEN_EX +"Masukkan password: ").strip()
    input_password = getpass.getpass("Password: ")
    hashed_password = hashlib.md5(input_password.encode()).hexdigest()
    sql_user = f"SELECT branchid FROM MsUser WHERE UserName = '{input_username}' AND Password = '{hashed_password}'"
    cursor = conn.cursor()
    cursor.execute(sql_user)
    rows = cursor.fetchall()
    
    if not rows:
        print(Fore.RED +"❌ Invalid username or password, please try again.\n")
        # main()
    else:
        clear()
        # global UserLogin  
        UserLogin = input_username.upper()
        BranchIDLogin = rows[0][0]
        cursor.close()
        print(f"Congratulations, {UserLogin}!")
        print("You have successfully logged in.\n")
    return rows

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

if __name__ == "__main__":
    main()
    clear()
    
