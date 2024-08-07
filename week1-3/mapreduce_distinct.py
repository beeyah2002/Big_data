from mrjob.job import MRJob

class MapReduce(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(",")
            datetime = data[2].strip()
            year = datetime.split(' ')[0].split('/')[2]
            # status_type = data[1].strip()
            # num_reaction = data[3].strip()
            date = datetime.split(' ')[0].split('/')[0]

            if year == '2018' :
                yield date, None

    def reducer(self, key, values):
        yield key, None

if __name__ == '__main__':
    MapReduce.run()