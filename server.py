# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import mysql.connector as mysql
import schedule
from uuid import uuid4
import numpy
import sys

import spacy
from spacy.language import Language

vectors_loc=("./vi.vec")

tmp_data = []
print("loading process");

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

def test_similarity_return_dictionary(nlp,product1, product2, matchId):
    tracking = {}
    docs = [nlp(product1), nlp(product2)]
    if (docs[0].similarity(docs[1]) >= 0.9):
        print('{:<10}\t{}\t{}'.format(docs[0].text, docs[1].text, docs[0].similarity(docs[1])))
    # if(len(product1) > 20): 
        tracking[matchId] = docs[0].similarity(docs[1]);  
    else: 
        tracking[matchId] = 0;
    return tracking;
                

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

   
def main():
    try:
        # enter your server IP address/domain name
        HOST = "127.0.0.1" # or "domain.com"
        # database name, if you want just to connect to MySQL server, leave it empty
        DATABASE = "shopping"
        # this is the user you create
        USER = "root"
        # user password
        PASSWORD = "Dangductrung@@123Th"
        # connect to MySQL server
        db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
        print("Connected to:", db_connection.get_server_info())
        # enter your code here!

        select_query ='select * from products'
        select_product_name = 'select name from products'
        select_all_id = 'select id from products'
        select_all_products_in_matches = 'select name, id from matches'
        cursor = db_connection.cursor(buffered=True)
        cursor.execute(select_query)
        records = cursor.fetchall()
        cursor.execute("select count(*) from products")  
        count = cursor.fetchone()[0]
        
        
        for row in records:
            filterNameProduct(row[1], row[0],row[5])

        for i in range(0,len(tmp_data)):
            newProduct = tmp_data[i];
            check_match_id = ("SELECT match_id FROM products WHERE id= " + str(newProduct['id']))
            cursor.execute(check_match_id)
            matchId = cursor.fetchone()[0]
            if(matchId is None): 
                cursor.execute("select count(*) from matches");
                countProductsInMatches = cursor.fetchone()[0];
                if(int(countProductsInMatches) == 0):  
                    add_record =  ("INSERT INTO matches "
                                    "(id,product_id,name) "
                                    "VALUES (%(id)s, %(product_id)s, %(name)s)")
                    data_matching_record = {
                                'id': 0,
                                'product_id': newProduct['id'],
                                'name':newProduct['name'],
                    }
                    cursor.execute(add_record,data_matching_record)  
                    select_id = 'SELECT id from matches where product_id =' + "'"  + str(newProduct['id']) + "'";
                    cursor.execute(select_id)
                    match_id = cursor.fetchone()[0]
                    insert_match_id = ("UPDATE products set match_id = " + "'" + str(match_id) +"'" + " where id = " + "'" + str(newProduct['id']) +"'") 
                    cursor.execute(insert_match_id)  
                else:
                    trackingIndex = {}
                    cursor.execute(select_all_products_in_matches);
                    all_products_in_matches = cursor.fetchall();
                    for product in all_products_in_matches:
                        # product có trong bảng match
                        productInMatches = product[0];
                        match_id=  product[1];
                        newItem = newProduct['name'];
                        matching = test_similarity_return_dictionary(nlp_model,productInMatches, newItem, match_id)
                        trackingIndex.update(matching)
                    if(bool(trackingIndex)):
                        max_key = max(trackingIndex, key=trackingIndex.get);
                        all_values = trackingIndex.values()
                        max_value = max(all_values)
                        if (max_value < 0.9): 
                            check_exist_id = 'SELECT count(*) from matches where product_id =' + "'"  + str(newProduct['id']) + "'";
                            cursor.execute(check_exist_id)
                            is_exist = cursor.fetchone()[0]
                            if(int(is_exist) == 0):
                                add_record =  ("INSERT INTO matches "
                                                "(id,product_id,name) "
                                                "VALUES (%(id)s, %(product_id)s, %(name)s)")
                                data_matching_record = {
                                            'id': 0,
                                            'product_id': newProduct['id'],
                                            'name':newProduct['name'],
                                        }
                                cursor.execute(add_record,data_matching_record)
                                select_id = 'SELECT id from matches where product_id =' + "'"  + str(newProduct['id']) + "'";
                                cursor.execute(select_id)
                                match_id = cursor.fetchone()[0]
                                insert_match_id = ("UPDATE products set match_id = " + "'" + str(match_id) +"'" + " where id = " + "'" + str(newProduct['id']) +"'") 
                                cursor.execute(insert_match_id)  
                        else: 
                            insert_match_id_B = ("UPDATE products set match_id = " + "'" + str(max_key) +"'" + " where id = " + "'" + str(newProduct['id']) +"'")   
                            cursor.execute(insert_match_id_B) 
            
                db_connection.commit()


    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if db_connection.is_connected():
            db_connection.close()
            cursor.close()
            print("MySQL connection is closed")

schedule.every(10).seconds.do(main)

while True:
    schedule.run_pending()