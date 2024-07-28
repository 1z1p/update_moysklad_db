import requests
import time
import pymysql
import json
import argparse    

connection = pymysql.connect(host='localhost',user='root',password='',db='parilka',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
parser = argparse.ArgumentParser()   

token = 'token'                                                                                                                                             
parser.add_argument("-type", "--type", help="Type", type=int)                                                                                                                                  
                                                                                                                                                                                               
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}                                                                                                             
args = parser.parse_args()                                                                                                                                                                     
                                                                                                                                                                                               
number_1_2_3 = [args.type]  

params = [
    ("filter", "updated>=2019-07-10 12:00:00;updated<=2019-07-12 12:00:00")
]
for item in number_1_2_3:
 
    url = f"https://api.moysklad.ru/api/remap/1.2/report/stock/bystore?filter=stockMode=all&offset={item}"
    response = requests.get(url=url, headers=headers)
    result = response.json()

    product = []

    for i in range(len(result["rows"])):
        url_product = result["rows"][i]["meta"]["href"]
        response_product = requests.get(url=url_product, headers=headers)
        result_product = response_product.json()

        cate = result_product["pathName"]
        parts = cate.split('/')
        categories = parts[0]
        f_categories = ""

        if(categories == "POD-SYSTEMS"):
            f_categories = "POD-Системы" 
        if(categories == "ОДНОРАЗОВЫЕ ЭЛ. СИГАРЕТЫ"):
            f_categories = "Odnorazki" 
        if(categories == "КОМПЛЕКТУЮЩИЕ К КАЛЬЯНУ"):
            f_categories = "Accessories_hookah" 
        if(categories == "КОМПЛЕКТУЮЩИЕ К POD"):
            f_categories = "POD_accessories" 
        if(categories == "КАЛЬЯНЫ"):
            f_categories = "Hookahs" 
        if(categories == "ЖИДКОСТИ"):
            f_categories = "Liquids"
        if(categories == "ТАБАК"):
            f_categories = "Tobacco"
        if(categories == "АКСЕССУАРЫ"):
            f_categories = "Accessories"

        if(categories == "КРОССОВКИ" or categories == "НАПИТКИ"):
            pass


        shops = []
        full_number = 0

        for item in range(len(result["rows"][i]["stockByStore"])):
            if(result["rows"][i]["stockByStore"][item]["name"] == "Оренбург опт"):
                shops.append({"shop": "Оренбург опт", "number": 0})
            else:
                shops.append({"shop": result["rows"][i]["stockByStore"][item]["name"], "number": int(result["rows"][i]["stockByStore"][item]["stock"])})

        for item in range(len(shops)):
            full_number += shops[item]["number"]

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM product WHERE article = %s", [result_product["code"]])
            price_v = int(result_product["salePrices"][0]["value"] / 100)

            if cursor.fetchone() is None and price_v >= 1:
                categories_list = ["POD-SYSTEMS", "ОДНОРАЗОВЫЕ ЭЛ. СИГАРЕТЫ", "КОМПЛЕКТУЮЩИЕ К КАЛЬЯНУ", "КОМПЛЕКТУЮЩИЕ К POD", "КАЛЬЯНЫ", "ЖИДКОСТИ", "ТАБАК", "АКСЕССУАРЫ"]
                if categories in categories_list:
                    print("[insert]")
                    shop = []
                    for item in result["rows"][i]["stockByStore"]:
                        if item["name"] == "Оренбург опт":
                            print("[STOCK]")
                            shop.append({"shop": "Оренбург опт", "number": 0})
                        else:
                            shop.append({"shop": item["name"], "number": int(item["stock"])})
                    json_string = json.dumps(shop, ensure_ascii=False)
                    cursor.execute(
                        "INSERT INTO product (image, name, article, price, shop, categories_id, avail) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        ("default.png", result_product["name"], result_product["code"], price_v, json_string, f_categories, full_number)
                    )
                else:
                    print("[pass]")
            else:
                shop = []
                for item in result["rows"][i]["stockByStore"]:
                    if item["name"] == "Оренбург опт":
                        shop.append({"shop": "Оренбург опт", "number": 0})
                    else:
                        shop.append({"shop": item["name"], "number": int(item["stock"])})
                price = f'{price_v}'
                json_string = json.dumps(shop, ensure_ascii=False)
                cursor.execute(
                    "UPDATE product SET shop = %s, price = %s, avail = %s WHERE article = %s",
                    (json_string, price, full_number, result_product["code"])
                )
                print(f'[Update] №{i} - {result_product["code"]} - {result_product["name"]} - {price_v}')

        connection.commit()
        time.sleep(2.5)
    time.sleep(4)