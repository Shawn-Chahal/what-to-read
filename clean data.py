import os
import time
from collections import Counter
import pandas as pd

N_USERS = 20000
STATUS_FREQUENCY = 30  # seconds

df_ratings = pd.read_csv(os.path.join('BX-CSV-Dump', 'BX-Book-Ratings.csv'), sep=';',
                         dtype={"User-ID": int, "ISBN": str, "Book-Rating": int})

df_ratings.drop([index for index, rating in zip(df_ratings.index, df_ratings['Book-Rating']) if rating == 0],
                inplace=True)

rating_min = df_ratings['Book-Rating'].min()
rating_max = df_ratings['Book-Rating'].max()

df_ratings['Book-Rating'] = df_ratings['Book-Rating'].map(
    lambda x: (x - rating_min) / (rating_max - rating_min) * 2 - 1)

df_count_user = pd.DataFrame.from_dict(Counter(df_ratings['User-ID'].to_list()), orient='index', columns=['count'])
df_count_user.sort_values(by=['count'], ascending=False, inplace=True)
df_count_user = df_count_user[:N_USERS]
df_ratings.drop(
    [index for index, user_id in zip(df_ratings.index, df_ratings['User-ID']) if user_id not in df_count_user.index],
    inplace=True)

set_isbn = sorted(set(df_ratings['ISBN']))
isbn_to_id = {j: i for i, j in enumerate(set_isbn)}
df_ratings['Book-ID'] = df_ratings['ISBN'].map(isbn_to_id)
df_ratings.to_csv(os.path.join('BX-CSV-Dump', 'BX-Book-Ratings-clean.csv'), index=False)

df_books = pd.read_csv(os.path.join('BX-CSV-Dump', 'BX-Books.csv'), sep=';', error_bad_lines=False,
                       dtype={"ISBN": str, "Book-Title": str, "Book-Author": str, "Year-Of-Publication": str,
                              "Publisher": str, "Image-URL-S": str, "Image-URL-M": str, "Image-URL-L": str})

df_books = df_books[['ISBN', 'Book-Title', 'Book-Author', 'Year-Of-Publication', 'Image-URL-L']]

drop_index = []
last_update = time.time()
progress = 0
for index, isbn in zip(df_books.index, df_books['ISBN']):
    if isbn not in set_isbn:
        drop_index.append(index)

    progress += 1
    if time.time() - last_update > STATUS_FREQUENCY:
        print(f'{progress / df_books.index.size: .1%}')
        last_update = time.time()

df_books.drop(drop_index, inplace=True)

df_books['Book-ID'] = df_books['ISBN'].map(isbn_to_id)
df_books.to_csv(os.path.join('BX-CSV-Dump', 'BX-Books-clean.csv'), index=False)
