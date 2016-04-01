import datetime
import itertools
import random
import timeit
from collections import defaultdict

import mongoengine as db

db.connect("test-dicts")

# Example to display usage of pymongo vs MongoEngine.
# Output-:
# pymongo took 0.26s
# ('mongoengine took', 21.69879698753357)


# It'll create a db "test-dicts" with collection name "my_model".
# Below example will create 5000 sample data.
# http://stackoverflow.com/questions/35257305/mongoengine-is-very-slow-on-large-documents-comapred-to-native-pymongo-usage
class MyModel(db.Document):
    date = db.DateTimeField(required=True, default=datetime.date.today)
    data_dict_1 = db.DictField(required=False)


MyModel.drop_collection()

data_1 = ['foo', 'bar']
data_2 = ['spam', 'eggs', 'ham']
data_3 = ["subf{}".format(f) for f in range(5)]

m = MyModel()
tree = lambda: defaultdict(tree)  # http://stackoverflow.com/a/19189366/3271558
data = tree()
for _d1, _d2, _d3 in itertools.product(data_1, data_2, data_3):
    data[_d1][_d2][_d3] = list(random.sample(range(50000), 20000))
m.data_dict_1 = data
m.save()


def pymongo_doc():
    return db.connection.get_connection()["test-dicts"]['my_model'].find_one()


def mongoengine_doc():
    return MyModel.objects.first()


if __name__ == '__main__':
    print("pymongo took {:2.2f}s".format(timeit.timeit(pymongo_doc, number=10)))
    print("mongoengine took", timeit.timeit(mongoengine_doc, number=10))
