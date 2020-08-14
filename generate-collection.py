from uri import uri
import time
import os
import pandas as pd
import pymongo

STATUS_FREQUENCY = 30  # seconds

myclient = pymongo.MongoClient(uri)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_books = mydb['books']

df_ratings = pd.read_csv(os.path.join('BX-CSV-Dump', 'BX-Book-Ratings-clean.csv'),
                         dtype={"User-ID": int, "Book-ID": int, "Book-Rating": float})
df_books = pd.read_csv(os.path.join('BX-CSV-Dump', 'BX-Books-clean.csv'),
                       dtype={"ISBN": str, "Book-Title": str, "Book-Author": str, "Year-Of-Publication": str,
                              "Image-URL-L": str, "Book-ID": int})

last_update = time.time()
progress = 0
user_ids = sorted(set(df_ratings['User-ID'].to_list()))
for user_id in user_ids:
    index_list = (df_ratings['User-ID'] == user_id)

    mydict = {'_id': user_id,
              'bookId': df_ratings['Book-ID'].loc[index_list].to_list(),
              'rating': df_ratings['Book-Rating'].loc[index_list].to_list()}

    mycol_ratings.insert_one(mydict)

    progress += 1
    if time.time() - last_update > STATUS_FREQUENCY:
        print(f'{progress / len(user_ids): .1%}')
        last_update = time.time()

last_update = time.time()
for i in range(df_books.index.size):
    mydict = {'_id': int(df_books['Book-ID'].iloc[i]),
              'Title': df_books['Book-Title'].iloc[i],
              'Author': df_books['Book-Author'].iloc[i],
              'Year': df_books['Year-Of-Publication'].iloc[i],
              'ISBN': df_books['ISBN'].iloc[i],
              'Image-URL': df_books['Image-URL-L'].iloc[i]}

    mycol_books.insert_one(mydict)

    if time.time() - last_update > STATUS_FREQUENCY:
        print(f'{i / df_books.index.size: .1%}')
        last_update = time.time()
