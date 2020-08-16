from uri import uri
import numpy as np
from scipy.sparse import csr_matrix
import pymongo

MIN_RATINGS = 40
SURVEY_LENGTH = 64
NUM_RESULTS = 32

NUM_USERS = 1000
NUM_TOP_MATCH = 50
BIAS = 1.0

rng = np.random.default_rng()

myclient = pymongo.MongoClient(uri, retryWrites=False)
mydb = myclient.get_default_database()
mycol_ratings = mydb['ratings']
mycol_books = mydb['books']
mydocs_survey = mycol_books.aggregate([{'$match': {'Ratings-Count': {'$gte': MIN_RATINGS}}},
                                       {'$sample': {'size': SURVEY_LENGTH}}])
book_survey_isbn = []
book_survey_title = []
book_survey_author = []
book_survey_image = []

for doc in mydocs_survey:
    book_survey_isbn.append(doc['_id'])
    book_survey_title.append(doc['Title'])
    book_survey_author.append(doc['Author'])
    book_survey_image.append(doc['Image-URL'])

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

    mydocs_similar_users = mycol_ratings.aggregate([{'$match': {'ISBN': {'$in': book_survey_isbn}}},
                                                    {'$sample': {'size': NUM_USERS}}])
    csr_userid = []
    csr_isbn = []
    csr_rating = []

    for document in mydocs_similar_users:
        csr_userid.extend([document['_id']] * len(document['rating']))
        csr_isbn.extend(document['ISBN'])
        csr_rating.extend(document['rating'])

    bookid_to_isbn = list(set(csr_isbn + book_survey_isbn))
    isbn_to_bookid = {isbn: bookid for bookid, isbn in enumerate(bookid_to_isbn)}
    csr_bookid = [isbn_to_bookid[isbn] for isbn in csr_isbn]
    user_bookid = [isbn_to_bookid[isbn] for isbn in book_survey_isbn]
    X = csr_matrix((csr_rating, (csr_userid, csr_bookid)), shape=(max(csr_userid) + 1, len(bookid_to_isbn) + 1))
    user_vector = csr_matrix((user_ratings, ([0] * len(user_ratings), user_bookid)),
                             shape=(1, len(bookid_to_isbn) + 1)).toarray()[0]

    match = X.dot(user_vector)
    match_idx = np.argsort(match)[::-1][:NUM_TOP_MATCH]
    match_sum = np.sum(match[match_idx])
    match_proba = np.reshape(match[match_idx] / (match_sum + 0.1) * NUM_TOP_MATCH, (-1, 1))

    results_nnz = X[match_idx].getnnz(axis=0)
    results_sum = X[match_idx].multiply(match_proba).sum(axis=0)
    results_vector = np.array((results_sum[0] / (results_nnz + BIAS)))[0]
    result_ids = np.argsort(results_vector)[::-1]

    results_count = 0
    percent_match = []
    book_title = []
    book_author = []
    book_isbn = []
    book_image = []

    for book_id in result_ids[:1000]:
        if user_vector[book_id] == 0:
            doc = mycol_books.find_one({'_id': bookid_to_isbn[book_id]})

            if doc is not None:
                percent_match.append(
                    f'{results_vector[book_id] / results_vector[result_ids[0]] * 0.99:3.0%} match')
                book_title.append(doc['Title'])
                book_author.append(doc['Author'])
                book_isbn.append(doc['_id'])
                book_image.append(doc['Image-URL'])

                results_count += 1
                if results_count == NUM_RESULTS:
                    break

    # RESULTS

    print('\nYou should check out:\n')

    for i in range(results_count):
        print(
            f'{percent_match[i]} | Counts: {results_nnz[isbn_to_bookid[book_isbn[i]]]}| '
            f'"{book_title[i]}" by {book_author[i]} \t| ISBN: {book_isbn[i]} | Cover: {book_image[i]}')
