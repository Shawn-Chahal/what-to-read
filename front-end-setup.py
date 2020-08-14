from uri import uri
from collections import Counter
import numpy as np
from scipy.sparse import csr_matrix
import pymongo


def valid_year(year):
    if len(year) != 4:
        return False

    numbers = str([i for i in range(10)])

    for c in year:
        if c not in numbers:
            return False

    return True


NUM_USERS = 1000
SURVEY_LENGTH = 50
NUM_TOP_BOOKS = 500
NUM_RESULTS = 25

NUM_TOP_MATCH = 50
BIAS = 0.3
MIN_COUNT = 2

rng = np.random.default_rng()

myclient = pymongo.MongoClient(uri, retryWrites=False)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_books = mydb['books']
mydocs_ratings = mycol_ratings.aggregate([{'$sample': {'size': NUM_USERS}}])

csr_userid = []
csr_isbn = []
csr_rating = []

for document in mydocs_ratings:
    doc_userid = document['_id']
    doc_isbn = document['ISBN']
    doc_rating = document['rating']

    csr_userid.extend([doc_userid] * len(doc_rating))
    csr_isbn.extend(doc_isbn)
    csr_rating.extend(doc_rating)

bookid_to_isbn = list(set(csr_isbn))
isbn_to_bookid = {isbn: bookid for bookid, isbn in enumerate(bookid_to_isbn)}

csr_bookid = [isbn_to_bookid[isbn] for isbn in csr_isbn]
books_count = Counter(csr_bookid)
books_count_keys = list(books_count.keys())
books_count_values = list(books_count.values())
book_counts = csr_matrix((books_count_values, ([0] * len(books_count_keys), books_count_keys))).toarray()[0]
max_bookid = book_counts.shape[0]
book_ids_top = np.argsort(book_counts)[::-1][:NUM_TOP_BOOKS]

book_survey_id = []
book_survey_title = []
book_survey_author = []

n_books_survey = 0
for book_id in rng.choice(book_ids_top, NUM_TOP_BOOKS, replace=False):

    doc = mycol_books.find_one({'_id': bookid_to_isbn[book_id]})
    if doc is not None:
        book_survey_id.append(int(book_id))
        book_survey_title.append(doc['Title'])
        book_survey_author.append(doc['Author'])
        n_books_survey += 1
        if n_books_survey >= SURVEY_LENGTH:
            break

# USER INPUT STARTS

print('\nDo you like the following books? y/n\n')
user_ratings = []
for title, author in zip(book_survey_title, book_survey_author):
    rating = input(f'"{title}" by {author}: ')
    if rating == 'y':
        user_ratings.append(1)
    elif rating == 'n':
        user_ratings.append(-1)
    else:
        user_ratings.append(0)

# USER INPUT ENDS

if sum([abs(i) for i in user_ratings]) == 0:
    print('Try again. Please rate at least one book.')

else:

    if max(book_survey_id) > max_bookid:
        max_bookid = max(book_survey_id)

    X = csr_matrix((csr_rating, (csr_userid, csr_bookid)), shape=(max(csr_userid) + 1, max_bookid + 1))

    user_vector = csr_matrix((user_ratings, ([0] * len(book_survey_id), book_survey_id)),
                             shape=(1, max_bookid + 1)).toarray()[0]

    match = X.dot(user_vector)
    match_idx = np.argsort(match)[::-1][:NUM_TOP_MATCH]
    match_sum = np.sum(match[match_idx])

    if match_sum == 0:
        match_sum = 1

    match_proba = np.reshape(match[match_idx] / match_sum * NUM_TOP_MATCH, (-1, 1))

    results_nnz = X[match_idx].getnnz(axis=0)
    results_sum = X[match_idx].multiply(match_proba).sum(axis=0)
    results_vector = np.array((results_sum[0] / (results_nnz + BIAS)))[0]
    result_ids = np.argsort(results_vector)[::-1]

    results_count = 0

    percent_match = []
    book_title = []
    book_author = []
    book_year = []
    book_isbn = []
    book_image = []

    for book_id in result_ids[:1000]:

        if user_vector[book_id] == 0:
            if results_nnz[book_id] >= MIN_COUNT:
                doc = mycol_books.find_one({'_id': bookid_to_isbn[book_id]})

                if doc is not None:

                    title = doc['Title']
                    author = doc['Author']
                    year = doc['Year'] if valid_year(doc['Year']) else 'Unavailable'
                    isbn = doc['_id']
                    image = doc['Image-URL']

                    percent_match.append(
                        f'{results_vector[book_id] / results_vector[result_ids[0]] * 0.99:3.0%} match')
                    book_title.append(title)
                    book_author.append(author)
                    book_year.append(year)
                    book_isbn.append(isbn)
                    book_image.append(image)

                    results_count += 1

                    if results_count == NUM_RESULTS:
                        break

    # RESULTS

    print('\nYou should check out:\n')

    for i in range(results_count):
        print(
            f'{percent_match[i]} | Counts: {results_nnz[isbn_to_bookid[book_isbn[i]]]}| "{book_title[i]}" by {book_author[i]} \t| Year: {book_year[i]} | ISBN: {book_isbn[i]} | Cover: {book_image[i]}')
