import pymssql
import datetime
from datetime import date, timedelta
import logging
import time
import calendar
_logger = logging.getLogger(__name__)


######## note revision ###########
# 1. update cek stock per product, jika stock kurang dari qty, maka insert ke tmp_iseller_additional
# 2. update HPP untuk per product, jika tidak ada hpp set ke 999.12

server = '127.0.0.1' #'196.168.168.96' # 
port = 1433
user = 'useresb'
password = 'mitoeasybuser201612'
database = 'MAJU'
today = datetime.date.today()
tahun = today.year
bulan = today.month
jumlah_hari = calendar.monthrange(tahun, bulan)[1]
start_time = datetime.datetime.now()
# tglMulai = (datetime.date.today() - timedelta(days=1)).day #ubah 1 untuk mulai darai awal bulan
tglMulai = datetime.date.today().day
tglAkhir = tglMulai + 1 ## jumlah_hari + 1 ## karnda menggunakan range, maka tglAkhir harus +1 dari jumlah hari Contoh: 31 hari, maka tglAkhir = 32 range(1,2) makan hanya 1 hari saja
with open("pushToEasyB_log.txt", "a", encoding="utf-8") as log_file:
    log_file.write("\nStart processing on " + str(datetime.datetime.now()) + ".\n")
    for day in range(tglMulai, tglAkhir):
        tgl = datetime.date(tahun, bulan, day)
    # date = '2025-07-01'
        try:
            conn2 = pymssql.connect(server=server, port=port, user=user, password=password, database=database)
            if not conn2:
                print('Koneksi GAGAL')
                log_file.write("\nConnecting on Conn2 failed " + str(datetime.datetime.now()) + ".\n")
                exit()
            else:
                print(f'Koneksi Datebase {database} Berhasil!!!!!!!!')
                log_file.write("\nConnecting on Conn2 success " + str(datetime.datetime.now()) + ".\n")
                
            cursor2 = conn2.cursor()
            date = str(tgl) or str(datetime.datetime.now().strftime("%Y-%m-%d"))
            yearMonth = datetime.datetime.now().strftime("%y%m")
            print(f"###### Processing date: {date} ######")
            skip_stores = [
                'TRENLY DADAP', 'TRENLY KARAWACI'
            ]
            sql_stores = """
                SELECT BranchName FROM MsBranch mb WHERE BranchName LIKE '%%trenly%%' and  BranchName NOT like 'TRENLY DADAP' order by BranchName
            """
            cursor2.execute(sql_stores)
            stores = cursor2.fetchall()
            log_file.write(f"\n{sql_stores}.\n")
            print("Execute for stores:", stores)
            idx_store = 0
            
            for store in stores:
                if store[0] in skip_stores:
                    print(f"Skipping store: {store[0]}")
                    continue

                conn = pymssql.connect(server=server, port=port, user=user, password=password, database=database)
                if not conn:
                    print('Koneksi GAGAL')
                    exit()
                else:
                    print(f'Koneksi Datebase {database} Berhasil!!!!!!!!')
                cursor = conn.cursor()
                BranchName = store[0]
                getDataSales = f"""
                    select CAST(order_date AS DATE) date, mb.BranchID, ml.LocationID, mc.CustomerID, 
                    mcl.CustomerLocationId, outlet_name outlet_name, product_id,mp.ProductID,
                    round((sum(subtotal_detail)- sum(tax_amount)) / SUM(quantity),2) 'Price List', sum(quantity)qty,
                    round(SUM(subtotal_detail),2)tot_price,
                    round(sum(isnull(tmp.amount_point,0)),2)amt_point,
                    round(sum(isnull(tmp.amount_voucher,0)),2)amt_voucher,
                    sku
                    from tmp_iseller_orders tmp
                    --join MsProduct mp on mp.field6 = tmp.product_id
                    join MsProduct mp on mp.field1 = tmp.sku
                    join MsBranch mb on mb.BranchName = tmp.outlet_name
                    join MsCustomer mc on TRY_CAST(mc.Website as INT) = mb.BranchID
                    join MsLocation ml on ml.BranchID = mb.BranchID and ml.LocationCode ='001'
                    join MsCustomerLocation mcl on mcl.CustomerID = mc.CustomerID
                    where 1=1 
                    and tmp.is_sync is null
                    and tmp.base_price > 1
                    --and tmp.product_name not like '%[[]GIFT POINT%'
                    --and tmp.product_name not like '%\[GIFT POINT%' ESCAPE '\'
                    --and tmp.order_reference ='#00046-34495-Trenly'
                    and CAST(order_date AS DATE) BETWEEN '{date}' and '{date}'
                    and mb.BranchName ='{BranchName}'
                    -- and product_id ='7bfb8880-4dc7-4756-a094-04cf178c8c8a'
                    group by CAST(order_date AS DATE),mb.BranchID,ml.LocationID,mc.CustomerID,
                    mcl.CustomerLocationId,mb.BranchID ,outlet_name,mp.ProductID,product_id,sku
                    --OPTION (USE HINT ('FORCE_DEFAULT_CARDINALITY_ESTIMATION'))
                """
                # '{TransactionDate}'       
                # print(getDataSales)
                # import ipdb; ipdb.set_trace()
                cursor.execute(getDataSales)
                sales_data = cursor.fetchall()

                if not sales_data:
                    print(f"No sales data found for {BranchName} on {date}. Skipping...")
                    continue

                getDataSalesTotal = f""" select sum(aa.tot_price)-(sum(aa.amt_point) + sum(aa.amt_voucher)) GrandTotal,(sum(aa.amt_point) + sum(aa.amt_voucher))AmountPointVoucher  from ({getDataSales}) aa  """
                cursor.execute(getDataSalesTotal)
                # print(getDataSalesTotal)
                time.sleep(1)
                DataTotal = cursor.fetchone()
                GrandTotal = DataTotal[0]
                AmountPointVoucher = round(round(DataTotal[1])/1.11,2) if DataTotal[1] else 0.0

                BranchID = sales_data[0][1]  # Assuming BranchID is the EasyB Branch ID
                LocationID = sales_data[0][2]  # Assuming LocationID is the EasyB Location ID
                CustomerID = sales_data[0][3]  # Assuming CustomerID is the EasyB Customer ID
                CustomerLocationID = sales_data[0][4]  # Assuming CustomerLocationID is the EasyB Customer Location ID
                outlet_name = sales_data[0][5]  # Assuming outlet_name is the EasyB Outlet Name

                try:
                    tmpDataGDV = []
                    idx = 0
                    tmp_prodct_ids = []
                    next_so_number = False
                    cnt_no_stock = 0
                    for sale in sales_data:
                        idx += 1
                        if BranchName.lower() == 'trenly karawaci':
                            BranchID = sale[1]  # Assuming BranchID is the EasyB Branch ID
                            BranchID = 63 
                            LocationID = 268 # assuming dadap as karawaci locationb ID
                            CustomerID = 15861 # A SRI SURYA NENGSIH / FITRAH ALDIANSYAH
                        sql_cek_stock = f"""
                            select COALESCE(sum(InQty)-sum(OutQty),0)stock from StockCard 
                            where 1=1
                            and ProductID={sale[7]} and locationID={LocationID}
                        """
                        cursor.execute(sql_cek_stock)
                        stock = cursor.fetchone()
                        # --select COALESCE(sum(qty),0)qty from StockHpp 
                        # --where ProductID={sale[7]} and locationID={LocationID}

                        qty_stock = stock[0] if stock else 0
                        if int(qty_stock) < int(sale[9]):
                            print(f"\nExecute for insert tmp_iseller_additional for product {sale[7]} stock {qty_stock} less than {sale[9]} on {BranchName} on {date}. Skipping to insert Sales Order and Delivery Order.")
                            Date = sale[0]
                            BranchID = sale[1]  # Assuming BranchID is the EasyB Branch ID
                            LocationID = sale[2]  # Assuming LocationID is the EasyB Location ID
                            CustomerID = sale[3]  # Assuming CustomerID is the EasyB Customer ID
                            CustomerLocationID = sale[4]  # Assuming CustomerLocationID is the EasyB Customer Location ID
                            OutletName = sale[5]  # Assuming outlet_name is the EasyB Outlet Name
                            IsellerProductId = sale[6]  # Assuming iseller_product_id is the EasyB Product ID
                            EasybProductID = sale[7]  # Assuming ProductID is the EasyB Product ID
                            PriceList = sale[8]  # Assuming PriceList is the price per unit
                            Qty = sale[9]  # Assuming Qty is the quantity sold
                            TotPrice = sale[10]  # Assuming TotPrice is the total amount
                            AmtPoint = sale[11]  # Assuming AmtPoint is the amount of point voucher
                            AmtVoucher = sale[12]  # Assuming AmtVoucher is the amount of voucher
                            sku = sale[13]
                            sql_insert = f"""
                                insert into tmp_iseller_additional(Date,BranchID,LocationID,CustomerID,CustomerLocationID,OutletName, IsellerProductId,EasybProductID,PriceList,Qty,TotPrice,AmtPoint,AmtVoucher)
                                OUTPUT INSERTED.ID
                                values('{Date}',{BranchID},{LocationID},{CustomerID},{CustomerLocationID},'{OutletName}','{IsellerProductId}',{EasybProductID},{PriceList},{Qty},{TotPrice},{AmtPoint},{AmtVoucher})
                            """
                            cursor.execute(sql_insert)

                            update_flag_sync = f"""
                                UPDATE tmp_iseller_orders
                                SET is_sync = 1
                                where sku = '{sku}' and cast(order_date as date) = '{date}' and outlet_name = '{OutletName}'
                            """
                            #print(f"Executing update sync flag: {update_flag_sync}")
                            cursor.execute(update_flag_sync)

                            log_file.write(f"\nExecute for insert tmp_iseller_additional for product {sale[7]} stock {qty_stock} less than {sale[9]} on {BranchName} on {date}.")
                            cnt_no_stock += 1

                            # if idx == 1:
                                # idx = 0
                            # continue
                        
                        ProductID = sale[7]  # Assuming ProductID is the EasyB Product ID
                        Price = sale[8] # Assuming Price is the price per unit
                        Qty = sale[9]  # Assuming Qty is the quantity sold
                        # Total = sale[10]  # Assuming Total is the total amount
                        Total = Price * Qty  # harus di hitung dari Price * Qty, kalau dari sale[10] itu totalnya tidak cocok denagan easyB
                        iseller_product_id = sale[6]
                        tmp_prodct_ids.append(iseller_product_id)

                        print("\r[+]", idx,BranchName, end='', flush=True)
                        
                        if idx == 1 or not next_so_number:
                            dateFormat = datetime.datetime.strptime(date, '%Y-%m-%d').date().strftime("%y%m%d")
                            current_datetime = sale[0]
                            # Sales Order Number
                            cursor.execute("""
                                SELECT TOP 1 TransNum, SUBSTRING(TransNum, 3, 100) AS seq
                                FROM SalesOrderHead
                                WHERE TransNum LIKE %s
                                ORDER BY TransNum DESC
                            """, (f'TRSO{dateFormat}%',))
                            last_so = cursor.fetchone()
                            next_so_number = f"TRSO{int(last_so[1][2:]) + 1:04d}" if last_so else f"TRSO{dateFormat}0001"


                            # Insert SO Head
                            TransNum = next_so_number
                            TDate = current_datetime
                            CustomerID = CustomerID  #9929 # A SRI SURYA NENGSIH / FITRAH ALDIANSYAH
                            SalesID = -1
                            BranchID = BranchID  # Assuming BranchID is the EasyB Branch ID
                            LocationID = LocationID  # Assuming LocationID is the EasyB Location ID
                            PaymentID = 2
                            COA = '1103.100'
                            BillingDay = 1
                            CustomerLocationID = CustomerLocationID # Assuming CustomerLocationID is the EasyB Customer Location ID
                            Currency = 'IDR'
                            Rate = 1
                            TaxRate = 1
                            GrandTotal = GrandTotal
                            Status = 4
                            AdditionalInfo = next_so_number
                            AuthorizationNotes = next_so_number
                            SalesOrderName = 'Sync Iseller'
                            SalesOrderApproval =  'Sync Iseller'
                            DivisionLevel1ID = None
                            DivisionLevel2ID = None
                            DivisionLevel3ID = None
                            DivisionLevel4ID = None
                            CreateBy = 'Sync Iseller'
                            CreateDate = current_datetime
                            EditBy = None
                            EditDate = None

                            cursor.execute("""
                                INSERT INTO SalesOrderHead (
                                    TransNum, TDate, CustomerID, SalesID, BranchID, LocationID, PaymentID, COA,
                                    BillingDay, CustomerLocationID, Currency, Rate, TaxRate, GrandTotal, Status,
                                    AdditionalInfo, AuthorizationNotes, SalesOrderName, SalesOrderApproval,
                                    CreateBy, CreateDate, EditBy, EditDate
                                ) 
                                VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """, (TransNum, TDate, CustomerID, SalesID, BranchID, LocationID, PaymentID, COA, BillingDay, CustomerLocationID, Currency, Rate, TaxRate, GrandTotal, Status, AdditionalInfo, AuthorizationNotes, SalesOrderName, SalesOrderApproval, CreateBy, CreateDate, EditBy, EditDate))

                            log_file.write(f"\nExecute for SO Head.")
                            # Delivery Order Number
                            dateFormatDO = datetime.datetime.strptime(date, '%Y-%m-%d').date().strftime("%y%m") 
                            # (datetime.datetime.now() + timedelta(hours=7)).strftime("%y%m")
                            cursor.execute("""
                                SELECT TOP 1 TransNum, SUBSTRING(TransNum, 5, 100) AS seq
                                FROM DeliveryOrderHead
                                WHERE TransNum LIKE %s
                                ORDER BY TransNum DESC
                            """, (f'TRDO{dateFormatDO}%',))
                            last_doh = cursor.fetchone()
                            next_doh_number = "TRDO"+ str(int(last_doh[1]) + 1) if last_doh else "TRDO" + dateFormatDO + "0000001"
                            # Insert DO Head
                            TransNum = next_doh_number
                            TDate = current_datetime
                            DueDate = current_datetime
                            PaymentID = 2
                            BranchID = BranchID
                            ProductType = 1
                            LocationID = LocationID
                            CustomerLocationID = CustomerLocationID
                            DeliveryInformation = TransNum + ' - ' + str(current_datetime)
                            GrandTotal = GrandTotal # Assuming amount_total is the GrandTotal
                            Status = 3 # 5 Assuming for 'goods delivered'
                            AdditionalInfo = TransNum + ' - ' + str(current_datetime)
                            AuthorizationNotes = TransNum + ' - ' + str(current_datetime)
                            DeliveryOrderName = 'Sync Iseller'
                            DeliveryOrderApproval = 'Sync Iseller'
                            # DivisionLevel1ID = False
                            # DivisionLevel2ID = False
                            # DivisionLevel3ID = False
                            # DivisionLevel4ID = False
                            CreateBy = 'Sync Iseller'
                            CreateDate = current_datetime

                            query_delivery_order_head = f"""
                                INSERT INTO DeliveryOrderHead (
                                    TransNum, TDate, DueDate, PaymentID, BranchID, ProductType,
                                    LocationID, CustomerLocationID, DeliveryInformation,
                                    GrandTotal, Status, AdditionalInfo, AuthorizationNotes,
                                    DeliveryOrderName, DeliveryOrderApproval,
                                    CreateBy, CreateDate
                                )
                                OUTPUT INSERTED.TransNum
                                VALUES ('{TransNum}','{TDate}','{DueDate}','{PaymentID}',{BranchID},{ProductType},{LocationID},{CustomerLocationID},'{DeliveryInformation}',{GrandTotal},{Status},'{AdditionalInfo}','{AuthorizationNotes}','{DeliveryOrderName}','{DeliveryOrderApproval}','{CreateBy}','{CreateDate}')
                            """
                            print("\nExecuting query_delivery_order_head")

                            cursor.execute(query_delivery_order_head)
                            next_doh_number = cursor.fetchone()[0]  # Get the new DO number
                            log_file.write(f"\nExecute for DO Head.")
                            # Map DO - SO
                            cursor.execute(
                                "INSERT INTO MapDeliverySO (DeliveryOrderID, SalesOrderID) VALUES (%s, %s)",
                                (next_doh_number, next_so_number)
                            )
                            log_file.write(f"\nExecute for Map DO - SO.")
                            ################ END SO HEAD ####################

                            ################ CUSTOMER EXPENSE ####################
                            if AmountPointVoucher > 0:
                                sql_customer_exprense = f"""
                                    insert into CustomerExpense(TransNum, TDate, CustomerID, Amount, Description,Status, Approval, CreateBy, CreateDate)VALUES ('{next_so_number}', '{current_datetime}', {CustomerID}, {AmountPointVoucher},'Sync Iseller', 3, 'Sync Iseller', 'Sync Iseller', '{current_datetime}')
                                    """
                                cursor.execute(sql_customer_exprense)
                                log_file.write(f"\nExecute for Customer Expense.")

                        #################### Insert Detail Line #################### 
                        # for line in self.order_line:
                        # (79, 186672, 1.0, 6000.0)
                        # print("+", sale)
                        print(f"\rInserted SalesOrderDetail with ID: {iseller_product_id}", end='', flush=True)
                        cursor.execute("""
                            INSERT INTO SalesOrderDetail (
                                TransNum, ProductID, UOMID, Qty, Price, Discount, PPN, TotalPrice, Notes, Status
                            )
                            OUTPUT INSERTED.ID
                            VALUES (%s, %s, 1, %s, %s, %s, %s, %s, '', 3)
                        """, (
                            next_so_number,
                            ProductID,  # Assuming sale[1] is the ProductID
                            Qty,
                            Price or 0,  # Price per unit
                            0,
                            0,
                            Total or 0,
                        ))
                        sodetail_id = cursor.fetchone()[0]
                        # status = 3 Assuming for 'goods delivered'
                        cursor.execute("""
                            INSERT INTO DeliveryOrderDetail (TransNum, SODetailID, Qty, Status)
                            OUTPUT INSERTED.ID
                            VALUES (%s, %s, %s, 3)
                        """, (next_doh_number, sodetail_id, Qty))
                        dodetail_id = cursor.fetchone()[0]
                        log_file.write(f"\nExecute for Delivery Order Detail.")

                        update_flag_sync = f"""
                            UPDATE tmp_iseller_orders
                            SET is_sync = 1
                            where product_id = '{iseller_product_id}' and cast(order_date as date) = '{date}' and outlet_name = '{outlet_name}'
                        """
                        #print(f"Executing update sync flag: {update_flag_sync}")
                        cursor.execute(update_flag_sync)
                        log_file.write(f"\nExecute for update sync flag.")
                        # cursor.execute(update_flag_sync, (sale[6], date, outlet_name))
                        tmpDataGDV.append((dodetail_id, ProductID, Qty, Total,iseller_product_id,Price))
                    print("\nWaiting, please do not close the program, Commiting Sales Order and Delivery Order")
                    conn.commit()
                    print(f"\nData {BranchName} has been inserted to SO: {next_so_number}, DO: {next_doh_number}, Total Lines: {len(sales_data)} \n")
                    print("Waiting for 10 seconds, please do not close the program, Continue to Goods Delivery Head and Detail")
                    log_file.write(f"\nCommit Sales Order and Delivery Order")
                    time.sleep(10)
                    # print("\n", tmpDataGDV)

                    RefNum = next_doh_number

                    update_delivery_order_head_to_deliverd = """
                        UPDATE DeliveryOrderHead
                        SET Status = 5 where Status = 3 and TransNum = %s
                    """
                    cursor.execute(update_delivery_order_head_to_deliverd, (RefNum,))
                    log_file.write(f"\nExecute for update Delivery Order Head.")
                    TDate = current_datetime
                    yearMonth = current_datetime.strftime('%Y%m')
                    yearMonth = datetime.datetime.strptime(date, '%Y-%m-%d').date().strftime('%Y%m')
                    sql_Goods_delivery = f"""
                        SELECT TOP 1 RIGHT(TransNum, 13)
                        FROM GoodsDeliveryHead
                        WHERE TransNum LIKE 'TR{yearMonth}%'
                        ORDER BY TransNum DESC
                    """
                    cursor.execute(sql_Goods_delivery)
                    # cursor.execute(sql_Goods_delivery, ('TR' + yearMonth + '%',))
                    row = cursor.fetchone()

                    if row and row[0]:
                        last_inv = row[0]
                        next_inv = 'TR' + str(int(last_inv) + 1)
                    else:
                        next_inv = 'TR' + yearMonth + '0000001'

                    TransNum = next_inv
                    RefNum = RefNum
                    TransType = 'Sales Delivery'
                    TDate = TDate
                    BranchID = BranchID
                    LocationID = LocationID
                    SenderID = 2
                    ShippingFee = 0
                    SubjectID = CustomerID
                    Driver = '-' 
                    PoliceNumber = '-' 
                    DeliveryInformation = ''
                    AdditionalInfo = next_inv
                    AuthorizationNotes = None
                    Status = 3  # Assuming 3 is the status for 'Done'
                    GoodsDeliveryName = 'Sync Iseller'
                    GoodsDeliveryApproval = 'Sync Iseller'
                    CreateBy = 'Sync Iseller'
                    CreateDate = TDate

                    query_goods_delivery_head = """
                    insert into GoodsDeliveryHead (TransNum, RefNum, TransType, TDate, BranchID, LocationID, SenderID, ShippingFee, SubjectID, Driver, PoliceNumber, DeliveryInformation, AdditionalInfo, AuthorizationNotes, Status, GoodsDeliveryName, GoodsDeliveryApproval, CreateBy, CreateDate) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query_goods_delivery_head, (TransNum, RefNum, TransType, TDate, BranchID, LocationID, SenderID, ShippingFee, SubjectID, Driver, PoliceNumber, DeliveryInformation, AdditionalInfo, AuthorizationNotes, Status, GoodsDeliveryName, GoodsDeliveryApproval, CreateBy, CreateDate))

                    log_file.write(f"\nExecute for Goods Delivery Head.")
                    # (dodetail_id, ProductID, Qty, Total,iseller_product_id,Price)
                    
                    sql_sales_invoice_detils = []
                    idx = 0
                    for data in tmpDataGDV:
                        idx += 1
                        sql_hpp = f"""
                            select StockDate, coalesce(qty,0)qty, coalesce(hpp,0)hpp from StockHpp 
                            where ProductID={data[1]} and locationID={LocationID}
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
                            need_stock = data[2]  # 10
                            remain_qty = data[2]  # 3
                            for shpp in res_HPP:
                                if shpp[1] >= remain_qty and need_stock == remain_qty: # 10 >= 3 and 10 == 3
                                    HPP = shpp[2]
                                    remain_qty = 0
                                    # tmp_hpp += tmp_qty * shpp[2]
                                    # HPP = tmp_hpp / data[2] if data[2] > 0 else 999.12
                                    break
                                
                                if int(shpp[1]) < int(remain_qty): # 7 < 10 remain_qty: # 7
                                    remain_qty -= shpp[1]
                                    tmp_hpp += shpp[1] * shpp[2] # 7 * 1000 = 7000
                                else:
                                    # tmp_qty = shpp[1] - remain_qty # 10 - 3 = 7
                                    tmp_hpp += remain_qty * shpp[2]
                                    remain_qty = 0
                                    HPP = tmp_hpp / data[2] if data[2] > 0 else 999.12
                                    break
                            if remain_qty > 0:
                                HPP = 999.12

                        SoID = int(data[0])
                        ProductID = data[1]
                        Qty = data[2]
                        Total = data[3]
                        # HPP = Total / Qty
                        iseller_product_id = data[4]
                        Price = data[5]
                        
                        RefDetailID = SoID  
                        ProductID = ProductID 
                        TransNum = TransNum
                        UOMID = 1
                        Qty = Qty
                        HPP = HPP
                        Notes = ''
                        Status = 1 # not invoiced yet, 4 is invoiced

                        query_goods_delivery_detail = """
                        INSERT INTO GoodsDeliveryDetail (TransNum,RefDetailID,ProductID,UOMID,Qty,ReceiveQty,ReturnQty,LostQty,HPP,Notes,Status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        # OUTPUT INSERTED.ID
                        cursor.execute(query_goods_delivery_detail, (TransNum, RefDetailID, ProductID, UOMID, Qty, Qty, 0, 0, HPP, '', Status))
                        cursor.execute("SELECT SCOPE_IDENTITY()")
                        delivery_detail_id = cursor.fetchone()[0]

                        sql_sales_invoice_detil = f"""
                        INSERT INTO SalesInvoiceDetail (TransNum, DeliveryDetailID, ProductID, UOMID, Qty, RealQty, Price, Discount, PPN, TotalPrice)
                        VALUES ('{TransNum}', {delivery_detail_id}, {ProductID}, {UOMID}, {Qty}, {Qty}, {Price}, 0, 11, {Total})
                        """
                        sql_sales_invoice_detils.append(sql_sales_invoice_detil)
                        print(f"\rCommit Data {BranchName} on line {idx} of {len(sales_data)}, Count no stock: {cnt_no_stock}" , end='', flush=True)
                        log_file.write(f"\nExecute for Goods Delivery Detail and Sales Invoice Detail then Commit")
                    conn.commit()

                    print("\nExecuting stored procedure sp_ApproveGoodsDelivery with TransNum:", TransNum)
                    cursor.execute("EXEC [dbo].[sp_ApproveGoodsDelivery] @TransNum = %s", TransNum)
                    print("Waiting for 10 seconds, please do not close the program, Continue to Sales Invoice Head and Detail")
                    log_file.write(f"\nExecute for sp_ApproveGoodsDelivery.")
                    time.sleep(10)
                    ############### sales invoice transaction ###############
                    TotalInvoice = GrandTotal

                    sql_sales_invoice_head = f"""
                    INSERT INTO SalesInvoiceHead (TransNum, TDate, BranchID, ProductType, LocationID, COA, DueDate, Currency, Rate, TaxRate, DiscountExtra, TotalInvoice, Receipt, FlagPrint, FlagTaxInvoice, Status, AdditionalInfo, AuthorizationNotes, InvoiceName, InvoiceApproval, CreateBy, CreateDate) VALUES ('{TransNum}','{TDate}','{BranchID}','1','-1',' ','{TDate}','IDR','1','1','{AmountPointVoucher}','{TotalInvoice}','1','0','1','3','{TransNum}','{AuthorizationNotes}','Sync Iseller','Sync Iseller','Sync Iseller','{TDate}')
                    """
                    cursor.execute(sql_sales_invoice_head)
                    log_file.write(f"\nExecute for Sales Invoice Head.")


                    updateLine = '#'
                    uid = 0
                    for sql in sql_sales_invoice_detils:
                        updateLine += '#'
                        uid += 1
                        # print("+",end=' ', flush=True)
                        print(f"\r{updateLine} [{uid}]", end='', flush=True)
                        time.sleep(0.1)
                        cursor.execute(sql)
                    print("\n")
                    log_file.write(f"\nExecute for Sales Invoice Detail.")
                    # select (sum(price*qty)-1311711.71)*1.11 from SalesInvoiceDetail WHERE TransNum ='TR2025070000024'
                    # if AmountPointVoucher > 0:
                    updateInvoiceHead = f"""
                        update SalesInvoiceHead set TotalInvoice = (select (sum(price*qty)-{AmountPointVoucher})*1.11 from SalesInvoiceDetail WHERE TransNum ='{TransNum}') where TransNum = '{TransNum}'
                    """
                    cursor.execute(updateInvoiceHead)
                        # select (sum(price*qty)-{AmountPointVoucher})*1.11 from SalesInvoiceDetail WHERE TransNum ='TR2025070000024'
                    log_file.write(f"\nExecute for update Sales Invoice Head.")

                    ########### update goods delivery detail status to invoiced = 4 ###########
                    sql_GoodsDeliveryHead_status = f"""
                        UPDATE GoodsDeliveryHead set Status = 4 where Status = 3 and TransNum = '{TransNum}';
                    """
                    cursor.execute(sql_GoodsDeliveryHead_status)
                    print(f"Executing SQL: {sql_GoodsDeliveryHead_status}")
                    log_file.write(f"\nExecute for update Goods Delivery Head.")

                    sql_GoodsDeliveryDetail_status = f"""
                        UPDATE GoodsDeliveryDetail set Status = 4 where Status = 1 and TransNum = '{TransNum}';
                    """
                    cursor.execute(sql_GoodsDeliveryDetail_status)
                    print(f"Executing SQL: {sql_GoodsDeliveryDetail_status}")
                    log_file.write(f"\nExecute for update Goods Delivery Detail.")

                    sql_MapSalesInvoiceDO_status = f"""
                        INSERT INTO MapSalesInvoiceDO (SalesInvoiceID,DeliveryOrderID)values('{TransNum}','{TransNum}')
                    """
                    cursor.execute(sql_MapSalesInvoiceDO_status)
                    print(f"Executing SQL: {sql_MapSalesInvoiceDO_status}")
                    log_file.write(f"\nExecute for Map Sales Invoice DO.")

                    print(f"\nExecuting stored procedure sp_ApproveSalesInvoice with TransNum: {TransNum}")
                    cursor.execute("EXEC [dbo].[sp_ApproveSalesInvoice] @TransNum = %s", TransNum) #--> auto inject ke CustomerReceivable
                    
                    ########### update is_sync = 1 ###########
                    sql_is_sync = f"""
                        UPDATE tmp_iseller_orders set is_sync = 1 where cast(order_date as date) = '{date}' and outlet_name = '{BranchName}';
                    """
                    cursor.execute(sql_is_sync)
                    
                    conn.commit()   
                    print(f"Finished processing {BranchName}, please do not close the program. Waiting for next store to be processed.")
                    idx_store += 1
                    log_file.write(f"\nExecute for sp_ApproveSalesInvoice.")
                    time.sleep(10) 

                except Exception as e:
                    print("Terjadi kesalahan:", e)
                    log_file.write(f"\nTerjadi kesalahan: {e}")
                    if 'conn' in locals():
                        conn.rollback()
                    #raise UserError(_("Database operation failed: %s") % str(e))
                finally:
                    if 'conn' in locals():
                        conn.close()

                print(f"{idx_store} Stores has been processed.")
                log_file.write(f"\n{idx_store} Stores has been processed.")
            end_time = datetime.datetime.now()
            elapsed_time = end_time - start_time
            print(f"Total execution time for the date: {date}: {elapsed_time}")
            log_file.write(f"\nTotal execution time for the date {date}: {elapsed_time}\n\n")
            conn2.close()

        except Exception as e:
            print("Terjadi kesalahan:", e)
            log_file.write(f"\nTerjadi kesalahan: {e}")
            if 'conn' in locals():
                conn.rollback()
                #raise UserError(_("Database operation failed: %s") % str(e))
        finally:
            if 'conn' in locals():
                conn.close()