
from mrjob.job import MRJob
from mrjob.step import MRStep


class TypeCounter(MRJob):

    def mapper(self, key, value):
        value_col = value.split(',')
        yield value_col[0], 1

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
# call the script using 'python test2.py mbti.csv' in the command line from the project folder
