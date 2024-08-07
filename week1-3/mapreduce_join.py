from mrjob.job import MRJob

class MapReduce(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(",")
            fbID = data[1]
            yield fbID , data

    def reducer(self, key, values):
        fb2 = []
        fb3 = []
        for i in values:
            if i[0] == 'FB2' :
                fb2.append(i)
            elif i[0] == 'FB3':
                fb3.append(i)

        for j in fb2 :
            for k in fb3 :
                yield None , (j + k)

if __name__ == '__main__':
    MapReduce.run()