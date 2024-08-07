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

        if len(fb2) > 0 :
            for i in fb2 :
                if len(fb3) > 0 :
                    for j in fb3 :
                        yield None , (i + j)
                elif len(fb3) == 0 :
                    yield None , i 
        if len(fb2) == 0:
            for k in fb3 :
                yield None , k



if __name__ == '__main__':
    MapReduce.run()