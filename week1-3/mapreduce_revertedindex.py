from mrjob.job import MRJob

class MapReduce(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(",")
            status_type = data[1].strip()
            num_reaction = data[3].strip()

            yield status_type, num_reaction

    def reducer(self, key, values):
        lval = []
        for react in values :
            lval.append(react)
        yield key, lval

if __name__ == '__main__':
    MapReduce.run()