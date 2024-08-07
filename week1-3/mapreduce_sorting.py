from mrjob.job import MRJob

class MapReduce(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(",")
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            num_reaction = data[3].strip()
            if int(num_reaction) > 3000 :  
                yield  year, num_reaction

    def reducer(self, key, values):
        # lval = []
        # lval.append(values)   
        sorting = sorted(values , reverse=True)
        yield key, sorting




if __name__ == '__main__':
    MapReduce.run()