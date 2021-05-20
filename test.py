# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import mysql.connector as mysql
import schedule
from uuid import uuid4
import numpy
import sys

import spacy
from spacy.language import Language

vectors_loc=("/Users/nghngtran/Desktop/Code/cc.vi.300.vec")

tmp_data = []

def load_nlp(vectors_loc, lang=None):
    if lang is None:
        nlp = Language()
    else:
        nlp = spacy.blank(lang)
    with open(vectors_loc, 'rb') as file_:
        header = file_.readline()
        nr_row, nr_dim = header.split()
        nlp.vocab.reset_vectors(width=int(nr_dim))
      
        for line in file_:
            line = line.rstrip().decode('utf8')
            pieces = line.rsplit(' ', int(nr_dim))
            word = pieces[0]
            vector = numpy.asarray([float(v) for v in pieces[1:]], dtype='f')
            nlp.vocab.set_vector(word, vector)  # add the vectors to the vocab
    return nlp


def test_similarity(nlp,product1, product2):
    docs = [nlp(product1['name']), nlp(product2['name'])]
    if (docs[0].similarity(docs[1]) >= 0.8):
        # print(product1['name'], product2['name'] + '----'+docs[0].similarity(docs[1]))
        print('{:<10}\t{}\t{}'.format(docs[0].text, docs[1].text, docs[0].similarity(docs[1])))
        return True
    else: 
        return False
                

nlp_model= load_nlp(vectors_loc)
print("load model successfully")

def filterNameProduct(productName, productId, matchId):
    check = False
    result_string = ""
    for i in range(len(productName)):
        if(productName[i] == "[" or productName[i] == "("): 
            check  = True
        if (productName[i] == ']' or productName[i] == ')'): 
            check = False
        elif (check == False):
          result_string += productName[i]
    product = {}
    product['name'] = result_string
    product['id']= productId
    product['match_id']= matchId
    tmp_data.append(product)

def checkMatchIdExistInTable(cursor,productId, tableName): 
    select_query = ("SELECT match_id FROM "+ tableName + " WHERE id= " + str(productId))
    cursor.execute(select_query)
    result = cursor.fetchone()[0]
    return result

def checkProductIsExistInTable(cursor,productId, tableName):
    select_query = ("SELECT id from "+ tableName +" where product_id = " + "'" + str(productId) + "'")
    print(select_query)
    cursor.execute(select_query)
    result = cursor.fetchone()[0]
    return result

def updateValueInTable(cursor, tableName, columnName, dataValue,condition):
    query = ("UPDATE " + tableName + " set " + columnName + "= " + str(dataValue) + "where id = " + str(condition))
    cursor.execute(query)
   
def match_product():
    try:
        # enter your server IP address/domain name
        HOST = "127.0.0.1" # or "domain.com"
        # database name, if you want just to connect to MySQL server, leave it empty
        DATABASE = "dummy"
        # this is the user you create
        USER = "root"
        # user password
        PASSWORD = "Ngoctran123"
        # connect to MySQL server
        db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
        print("Connected to:", db_connection.get_server_info())
        # enter your code here!

        select_query ='select * from products_clone'
        select_product_name = 'select name from products_clone'
        select_all_id = 'select id from products_clone'
        cursor = db_connection.cursor(buffered=True)
        cursor.execute(select_query)
        records = cursor.fetchall()
        cursor.execute("select count(*) from products_clone")  
        count = cursor.fetchone()[0]
        for row in records:
            filterNameProduct(row[1], row[0],row[5])

        for i in range(0,len(tmp_data)):
            for j in range(i+1, len(tmp_data)):
                productA = tmp_data[i]
                productB = tmp_data[j]
                matching = test_similarity(nlp_model,productA, productB)
                matching_id = ''
                exist_item = ''
                # nếu match
                match_id_A = ("SELECT match_id FROM products_clone WHERE id= " + str(productA['id']))
                match_id_B = ("SELECT match_id FROM products_clone WHERE id= " + str(productB['id']))
                cursor.execute(match_id_A)
                a = cursor.fetchone()[0]
                cursor.execute(match_id_B)
                b = cursor.fetchone()[0]
            
                # a = checkMatchIdExistInTable(cursor, productA['id'], "products_clone")
                # b = checkMatchIdExistInTable(cursor, productB['id'], "products_clone")
                if (matching):
                    check_exist_item_A = a is not None
                    check_exist_item_B = b is not None
                    if(check_exist_item_A and check_exist_item_B == False): 
                        insert_match_id = ("Update products_clone set match_id = %s where id = %s")
                        input_data = (a, productB['id'])
                        cursor.execute(insert_match_id,input_data)
                    # neu B ton tai
                    elif(check_exist_item_B and check_exist_item_A == False):
                        insert_match_id = ("Update products_clone set match_id = %s where id = %s")
                        input_data = (b, productA['id'])
                        cursor.execute(insert_match_id,input_data)
                    # neu chua ton tai match_id
                    elif(check_exist_item_A == False and check_exist_item_B == False): 
                        add_record =  ("INSERT INTO matches_clone"
                                "(id,product_id) "
                                "VALUES (%(id)s, %(product_id)s)")
                        data_matching_record = {
                            'id': 0,
                            'product_id': productA['id']
                        }
                        cursor.execute(add_record,data_matching_record)

                        match_id = ("SELECT id from matches_clone where product_id" + "= " + "'" + str(productA['id']) + "'")
                    
                        cursor.execute(match_id)
                        tmp = cursor.fetchone()[0]
                        # tmp = checkProductIsExistInTable(cursor,productA['id'], "matches_clone")
                        
                        insert_match_id_A = ("UPDATE products_clone set match_id = " + "'" + str(tmp) +"'" + " where id = " + "'" + str(productA['id']) +"'")
                        
                        cursor.execute(insert_match_id_A)

                        insert_match_id_B = ("UPDATE products_clone set match_id =" + "'" + str(tmp) +"'" + " where id = " + "'" + str(productB['id']) +"'")
                        cursor.execute(insert_match_id_B)
                    elif(check_exist_item_A == True and check_exist_item_B == True): 
                        match_id = ("SELECT id from matches_clone where product_id" + "= " + "'" + str(productA['id']) + "'")
                    
                        cursor.execute(match_id)
                        tmp = cursor.fetchone()
                        # tmp = checkProductIsExistInTable(cursor,productA['id'], "matches_clone")
                        if(tmp is not None):
                            insert_match_id_B = ("UPDATE products_clone set match_id = " + "'" + str(tmp[0]) +"'" + " where id = " + "'" + str(productB['id']) +"'")
                            
                            cursor.execute(insert_match_id_B)

                            delete_duplicate_match_id = ("DELETE FROM matches_clone WHERE product_id=" + str(productB['id']))
                            cursor.execute(delete_duplicate_match_id)
                        # updateValueInTable(cursor, "products_clone","match_id", tmp, productA['id'])
                        # updateValueInTable(cursor, "products_clone","match_id", tmp, productB['id'])
                # neu ko match
                else: 
                    check_exist_item_A = a is not None
                    check_exist_item_B = b is not None
                    add_record =  ("INSERT INTO matches_clone "
                                    "(id,product_id) "
                                    "VALUES (%(id)s, %(product_id)s)")
                    # if a have not match_id
                    if(check_exist_item_A and check_exist_item_B == False):
                        data_matching_record = {
                            'id': 0,
                            'product_id': productB['id']
                        }
                        cursor.execute(add_record,data_matching_record)
                        match_id = ("SELECT id from matches_clone where product_id" + "= " + "'" + str(productB['id']) + "'")
                    
                        cursor.execute(match_id)
                        tmp = cursor.fetchone()[0]
                        
                        insert_match_id_B = ("UPDATE products_clone set match_id = " + "'" + str(tmp) +"'" + " where id = " + "'" + str(productB['id']) +"'")
                        
                        cursor.execute(insert_match_id_B)

                        
                        # updateValueInTable(cursor, "products_clone","match_id", tmp, productA['id'])
                    elif(check_exist_item_B and check_exist_item_A == False):
                        data_matching_record = {
                            'id': 0,
                            'product_id': productA['id']
                        }
                        cursor.execute(add_record,data_matching_record)
                        match_id = ("SELECT id from matches_clone where product_id" + "= " + "'" + str(productA['id']) + "'")
                    
                        cursor.execute(match_id)
                        tmp = cursor.fetchone()[0]
                        insert_match_id_A = ("UPDATE products_clone set match_id =" + "'" + str(tmp) +"'" + " where id = " + "'" + str(productA['id']) +"'")
                        cursor.execute(insert_match_id_A)
                        # updateValueInTable(cursor, "products_clone","match_id", tmp, productB['id'])
                    elif(check_exist_item_B == False and check_exist_item_A == False):
                        data_matching_record = {
                            'id': 0,
                            'product_id': productA['id']
                        }
                        cursor.execute(add_record,data_matching_record)
                        match_id = ("SELECT id from matches_clone where product_id" + "= " + "'" + str(productA['id']) + "'")
                    
                        cursor.execute(match_id)
                        tmp = cursor.fetchone()[0]

                        insert_match_id_A = ("UPDATE products_clone set match_id =" + "'" + str(tmp) +"'" + " where id = " + "'" + str(productA['id']) +"'")
                        cursor.execute(insert_match_id_A)

                        data_matching_record = {
                            'id': 0,
                            'product_id': productB['id']
                        }
                        cursor.execute(add_record,data_matching_record)
                        match_id = ("SELECT id from matches_clone where product_id" + "= " + "'" + str(productB['id']) + "'")
                    
                        cursor.execute(match_id)
                        tmp = cursor.fetchone()[0]

                        insert_match_id_B = ("UPDATE products_clone set match_id =" + "'" + str(tmp) +"'" + " where id = " + "'" + str(productB['id']) +"'")
                        cursor.execute(insert_match_id_B)
                db_connection.commit()   
                
            

    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if db_connection.is_connected():
            db_connection.close()
            cursor.close()
            print("MySQL connection is closed")


schedule.every(10).minutes.do(match_product)

while True:
    schedule.run_pending()