from uri import uri
import os
from collections import Counter
import pandas as pd
import pymongo

N_USERS = 10000
STATUS_FREQUENCY = 30  # seconds

df_ratings = pd.read_csv(os.path.join('BX-CSV-Dump', 'BX-Book-Ratings.csv'), sep=';',
                         dtype={"User-ID": int, "ISBN": str, "Book-Rating": int})

df_ratings.drop([index for index, rating in zip(df_ratings.index, df_ratings['Book-Rating']) if rating == 0],
                inplace=True)

rating_min = df_ratings['Book-Rating'].min()
rating_max = df_ratings['Book-Rating'].max()

df_ratings['Book-Rating'] = df_ratings['Book-Rating'].map(
    lambda x: (x - rating_min) / (rating_max - rating_min) * 2 - 1)

top_users = [user_id for user_id, count in Counter(df_ratings['User-ID'].to_list()).most_common(N_USERS)]

df_ratings.drop(
    [index for index, user_id in zip(df_ratings.index, df_ratings['User-ID']) if user_id not in top_users],
    inplace=True)

set_isbn = set(df_ratings['ISBN'].to_list())
n_book_ratings = Counter(df_ratings['ISBN'].to_list())

df_ratings.to_csv(os.path.join('BX-CSV-Dump', 'BX-Book-Ratings-clean.csv'), index=False)

df_books = pd.read_csv(os.path.join('BX-CSV-Dump', 'BX-Books.csv'), sep=';', error_bad_lines=False,
                       dtype={"ISBN": str, "Book-Title": str, "Book-Author": str, "Year-Of-Publication": str,
                              "Publisher": str, "Image-URL-S": str, "Image-URL-M": str, "Image-URL-L": str})

df_books = df_books[['ISBN', 'Book-Title', 'Book-Author', 'Image-URL-L']]
df_books['Ratings-Count'] = df_books['ISBN'].map(n_book_ratings)
df_books.drop([index for index, isbn in zip(df_books.index, df_books['ISBN']) if isbn not in set_isbn], inplace=True)
df_books.to_csv(os.path.join('BX-CSV-Dump', 'BX-Books-clean.csv'), index=False)

myclient = pymongo.MongoClient(uri)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_books = mydb['books']

user_ids = sorted(set(df_ratings['User-ID'].to_list()))

mydocs_ratings = []
for user_id in user_ids:
    index_list = (df_ratings['User-ID'] == user_id)

    mydict = {'_id': user_id,
              'ISBN': df_ratings['ISBN'].loc[index_list].to_list(),
              'rating': df_ratings['Book-Rating'].loc[index_list].to_list()}

    mydocs_ratings.append(mydict)

mydocs_books = []
for i in range(df_books.index.size):
    mydict = {'_id': df_books['ISBN'].iloc[i],
              'Title': df_books['Book-Title'].iloc[i],
              'Author': df_books['Book-Author'].iloc[i],
              'Ratings-Count': int(df_books['Ratings-Count'].iloc[i]),
              'Image-URL': df_books['Image-URL-L'].iloc[i]}

    mydocs_books.append(mydict)

print('Uploading ratings to database...')
mycol_ratings.insert_many(mydocs_ratings)
print('Ratings uploaded.')

print('Uploading book metadata to database...')
mycol_books.insert_many(mydocs_books)
print('Book metadata uploaded.')
