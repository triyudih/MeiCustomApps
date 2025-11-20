import requests
import pymssql
import json
import datetime
from datetime import date, timedelta
import logging
import time

########## update 2025-08-19 ##########
# prorate amount point dan amount voucher dibagi ke item yang base_price > 1
# jika base_price <= 1, maka amount point dan amount voucher = 0
# sehingga ketika di inject sales order di easyb product Triger dan promo tidak terbawa

# koneksi esyb
server = '127.0.0.1'
port = 1433
user = 'useresb'
password = 'mitoeasybuser201612'
database = 'MAJU'

# mulai = 20
# akhir = 20 # jumlah hari yang ingin diambil, misal 1 untuk hari ini, 2 untuk hari ini dan kemarin, dst.
# for mulai in range(mulai, akhir):

conn = pymssql.connect(server=server, port=port, user=user, password=password, database=database)
cursor = conn.cursor()

sql_iseller_token = f"""SELECT Token FROM tmp_iseller_token WHERE id= 1 AND Token IS NOT NULL"""
cursor = conn.cursor()  
cursor.execute(sql_iseller_token)
token = cursor.fetchone()

client_id = "b375e8a5a0f145fb81a67ebc5bf16c18"
client_secret = "secret-a7cGFeCXwTacKhEQ2QFZ/gcBOo/QCmVLCpSIqzO6dSOZVKH38+K8M2uUbAbYdt3g8oulolow1VLzvxfp8BJ1lAF4ze4Y+HngsjlQytQV62I="
# refresh_token = "zGuvq2jrFv1hyqZ8jq7uCswDcwY16A6X4Iu98sbuVj83Kp5VQyFEhMKzZRWa_iJQQe3jCFDV7RxooZhVSVeNbZQ505tFitrArl8GawWPwoxk3QzZmPxPV2mFEv8EIk8L9GVlUUPWZm2m98rBii6MAuD0g8cFEddUhnydqKgosfjnm_p4GpRTqi0et_ZNGBljsKUCddnQ8dfpQ6-5442CXPziNJBvFwUw25RTMvFdVUMZGlCVU5h3YfHdC4ynBoVrB2JcBp060PH3G4Erd9OtXYfJJTwc7Y4sgLw7dL3FyLx404SE4UG2qGP-El4w0eRAl_4I4OlxFzCDst4Irlh_ivaIRJdRMPoIHyfvoqfXqlxWS3CCRminrR24FAeefCzQwRETiZ_TSWNJhAPnHHVSL0TX4aapCJXzzDeod-ZYiTOJWreQ2Y9Ozy344hLDOP18tQ2AUIVc-QQLgyvf88ef9jyNkohzgrtSAWQAG3wg7nc3iRb2"  # biasanya token panjang dibagi menjadi access_token dan refresh_token

refresh_token = token[0] if token else None
url = "https://trenly.isellershop.com/api/v2/GetOrders"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    # "client-id": client_id,
    # "client-secret": client_secret, 
    "Authorization": "Bearer " + refresh_token  # Biasanya token akses digunakan di sini
}

# date_process = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
date_process = datetime.date.today().strftime('%Y-%m-%d')
# date_process = '2025-10-17'
# date_process = f'2025-08-{"0"+str(mulai) if mulai < 10 else mulai}'   # ubah klo ingin manual
payload = {
    "time_zone": 7,
    "modified_after": date_process+"T00:05:00",
    "modified_before": date_process+"T23:59:59",
    "includes": "OrderDetails,discountdetails,promotiondetails,trackinventory,transactions",
    #"payment_status": "paid",
}
has_next_item = True
page = 0
order_id_exists = []
idx = 0
print("Connect to ", database)
log_file = open("log_output.txt", "a", encoding="utf-8") 
log_file.write("\nStart processing on " + str(datetime.datetime.now()) + ".\n")
log_file.write("Connecting to " + database + ".\n")

sql_cek_order_exist = """
    SELECT distinct(order_id) as order_id FROM tmp_iseller_orders WHERE cast(order_date as date) = '%s'
""" % (date_process)

cursor.execute(sql_cek_order_exist)
order_exist = [row[0] for row in cursor.fetchall()] 
conn.close()
if order_exist:
    print(f"Found {len(order_exist)} existing orders for {date_process}.")
    log_file.write("Checking existing orders for " + date_process + ". Found " + str(len(order_exist)) + " existing orders.\n")
    raise ValueError(f"Found {len(order_exist)} existing orders for {date_process}.")
# amount_point = 0
# amount_voucher = 0
while has_next_item:
    payload['page'] = page
    print("\n############ ",payload )
    print("Please wait, processing page", page, "of orders...")
    conn = pymssql.connect(server=server, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    response = requests.post(url, data=payload, headers=headers)
    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.text}")
        log_file.write("Error " + str(response.status_code) +" "+ response.text + " on " + str(datetime.datetime.now()) + ".\n")
        break
    else:
        data = response.json()
        amount_point = 0
        amount_voucher = 0
        for order in data.get('orders', []):
            log_file.write(str(order) + "\n")
            if order['order_id'] in order_exist:
                print(f"\r######## Order {order['order_id']} already processed, skipping...", end='', flush=True)
                continue
            order_id = order['order_id']
            order_reference = order['order_reference']
            order_date = order['order_date']
            outlet_id = order['outlet_id']
            outlet_name = order['outlet_name']
            outlet_code = order['outlet_code']
            customer_id = order['customer_id'] if order.get('customer_id') else False
            customer_first_name = str(order['customer_first_name']).replace("'","`") if order.get('customer_first_name') else ""
            customer_last_name = str(order['customer_last_name']).replace("'","`") if order.get('customer_last_name') else ""
            customer_phone_number = order['customer_phone_number'] if order.get('customer_phone_number') else ""
            customer_email = order['customer_email'] if order.get('customer_email') else ""
            total_order_amount_header = order['total_order_amount']
            discount_percentage_header = order['discount_percentage'] or 0
            total_discount_amount = order['total_discount_amount']
            total_promotion_amount = order['total_promotion_amount']
            subtotal_header = order['subtotal']
            total_tax_amount = order['total_tax_amount']
            total_additional_final_amount = order['total_additional_final_amount']
            total_additional_subtotal_amount = order['total_additional_subtotal_amount']
            total_additional_order_amount = order['total_additional_order_amount']
            rounding_amount = order['rounding_amount'] 
            total_amount = order['total_amount'] 
            total_bonus = order['total_bonus'] or 0  
            buying_price = order['buying_price']    
            created_at = date_process
            # amount_point = sum(t.get("amount", 0) for t in order.get("transactions", []) if t.get("gateway") == "points") / len([odr for odr in order['order_details'] if odr['base_price'] > 1]) if order.get("transactions") else 0
            # amount_voucher = sum(t.get("amount", 0) for t in order.get("transactions", []) if t.get("gateway") == "voucher") / len([odr for odr in order['order_details'] if odr['base_price'] > 1]) if order.get("transactions") else 0
            amount_point = sum(t.get("amount", 0) for t in order.get("transactions", []) if t.get("gateway") == "points") 
            #if order.get("transactions") else 0
            amount_voucher = sum(t.get("amount", 0) for t in order.get("transactions", []) if t.get("gateway") == "voucher") 
            #if order.get("transactions") else 0
            tot_items = len([odr for odr in order['order_details'] if odr['base_price'] > 1]) or 0
            
            try:
                # if len(order['order_details']) > 1:
                #     print(f">> Order {order_id} has more than one order detail, processing...")
                for detil in order['order_details']:
                    order_detail_id = detil['order_detail_id']
                    product_id = detil['product_id']
                    product_name = str(detil['product_name']).replace("'", '`')
                    sku = detil['sku']
                    quantity = detil['quantity']
                    base_price = detil['base_price']
                    total_order_amount_detail = detil['total_order_amount']
                    subtotal_detail = detil['subtotal']
                    discount_amount = detil['discount_amount']
                    discount_percentage_detail = detil['discount_percentage'] if detil.get('discount_percentage') else 0
                    discount_order_amount = detil['discount_order_amount']
                    tax_percentage = detil['tax_percentage']
                    tax_amount = detil['tax_amount']
                    prorate_amount_point = amount_point / tot_items if tot_items else 0
                    prorate_amount_voucher = amount_voucher / tot_items if tot_items else 0
                    if detil['base_price'] <= 1:
                        prorate_amount_point = 0
                        prorate_amount_voucher = 0

                    query_insert = f"""
                    INSERT INTO tmp_iseller_orders (order_id, order_reference, order_date, outlet_id, outlet_name, outlet_code, customer_id, customer_first_name, customer_last_name, customer_phone_number, customer_email, total_order_amount_header, discount_percentage_header, total_discount_amount, total_promotion_amount, subtotal_header, total_tax_amount, total_additional_final_amount, total_additional_subtotal_amount, total_additional_order_amount, rounding_amount, total_amount, total_bonus, buying_price, order_detail_id, product_id, product_name, sku, quantity, base_price, total_order_amount_detail, subtotal_detail, discount_amount, discount_percentage_detail, discount_order_amount, tax_percentage, tax_amount,created_at,amount_point, amount_voucher)
                    VALUES ('{order_id}', '{order_reference}', '{order_date}', '{outlet_id}', '{outlet_name}', '{outlet_code}', '{customer_id}', '{customer_first_name}', '{customer_last_name}', '{customer_phone_number}', '{customer_email}', {total_order_amount_header}, {discount_percentage_header}, {total_discount_amount}, {total_promotion_amount}, {subtotal_header}, {total_tax_amount}, {total_additional_final_amount}, {total_additional_subtotal_amount}, {total_additional_order_amount}, {rounding_amount}, {total_amount}, {total_bonus}, {buying_price}, '{order_detail_id}', '{product_id}', '{product_name}', '{sku}', {quantity}, {base_price}, {total_order_amount_detail}, {subtotal_detail}, {discount_amount}, {discount_percentage_detail}, {discount_order_amount}, {tax_percentage}, {tax_amount}, {created_at}, {prorate_amount_point}, {prorate_amount_voucher});
                    """
                    #print({order_reference}')
                    cursor.execute(query_insert)
                conn.commit() 
            except Exception as e:
                print(f"####### Error: {e}")    
                print(f"\n\n {query_insert}\n\n")    
                log_file.write(f"Error {e} on " + str(datetime.datetime.now()) + ".\n")
                conn.rollback()
                break
            idx += 1
            print(f"\r>> Processed order detail {idx} for order {order_id}", end='', flush=True)

        # sql_insert_customer_expense = """
        #     insert into CustomerExpense (TransNum,TDate,CustomerID,Amount,)values()
        # """
        # cursor.execute(sql_insert_customer_expense)
        # print(f"\r>> Processed {idx} orders for page {page}", end='', flush=True)


        # import ipdb; ipdb.set_trace()
        has_next_item = data.get('has_next_item')
    page += 1
conn.commit()
# cek_total_insert = f"""
#     SELECT COUNT(*) FROM tmp_iseller_orders where cast(order_date as date) = '{date_process}'
# """
# cursor.execute(cek_total_insert)
# total_inserted = cursor.fetchone()[0]
# print(f"\n\nTotal orders inserted for {date_process}: {total_inserted}\n\n")
cursor.close()
print("End Processed on " + str(datetime.datetime.now()) + ".\n")
log_file.write("End Processed on " + str(datetime.datetime.now()) + ".\n")

# CREATE TABLE tmp_iseller_orders (
#    id INT,
#    order_id VARCHAR(100),
#    order_reference VARCHAR(100),
#    order_date DATETIME,
#    outlet_id VARCHAR(100),
#    outlet_name VARCHAR(100),
#    outlet_code VARCHAR(100),
#    customer_id VARCHAR(100),
#    customer_first_name VARCHAR(100),
#    customer_last_name VARCHAR(100),
#    customer_phone_number VARCHAR(50),
#    customer_email VARCHAR(100),
#    total_order_amount_header DECIMAL(18, 6),
#    discount_percentage_header DECIMAL(5, 2),
#    total_discount_amount DECIMAL(18, 6),
#    total_promotion_amount DECIMAL(18, 6),
#    subtotal_header DECIMAL(18, 6),
#    total_tax_amount DECIMAL(18, 6),
#    total_additional_final_amount DECIMAL(18, 6),
#    total_additional_subtotal_amount DECIMAL(18, 6),
#    total_additional_order_amount DECIMAL(18, 6),
#    rounding_amount DECIMAL(18, 6),
#    total_amount DECIMAL(18, 6),
#    total_bonus DECIMAL(18, 6),
#    buying_price DECIMAL(18, 6),
#    order_detail_id VARCHAR(100),
#    product_id VARCHAR(100),
#    product_name NVARCHAR(510),
#    sku VARCHAR(100),
#    quantity DECIMAL(18, 6),
#    base_price DECIMAL(18, 6),
#    total_order_amount_detail DECIMAL(18, 6),
#    subtotal_detail DECIMAL(18, 6),
#    discount_amount DECIMAL(18, 6),
#    discount_percentage_detail DECIMAL(5, 2),
#    discount_order_amount DECIMAL(18, 6),
#    tax_percentage DECIMAL(5, 2),
#    tax_amount DECIMAL(18, 6),
#    is_sync BIT,
#    created_at DATETIME,
#    updated_at DATETIME,
#    amount_point DECIMAL(18, 6),
#    amount_voucher DECIMAL(18, 6)
# );