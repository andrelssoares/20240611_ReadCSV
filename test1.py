import csv

from mrjob.job import MRJob
from mrjob.step import MRStep


class TypeCounter(MRJob):

    def mapper(self, key, value):
        result = next(csv.reader([value], quotechar=None))
        mbti_type = result[0]
        yield mbti_type, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    TypeCounter.run()

# call the script using 'python test1.py mbti.csv' in the command line from the project folder
