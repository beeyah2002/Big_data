from mrjob.job import MRJob
from mrjob.step import MRStep



class MapReduce(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(",")
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip()
            num_reaction = data[3].strip()
                
            if year == '2018' and int(num_reaction) > 2000:
                if status_type == 'photo' :
                    yield 'Photo', num_reaction
                elif status_type == 'video' :
                    yield 'video', num_reaction
                elif status_type == 'link' :
                    yield 'link', num_reaction
                elif status_type == 'status' :
                    yield 'status', num_reaction   

    # def reducer_count(self, key, values):
    #     yield key[0], (sum(values), key[1])

    

    # def steps(self):
    #     return [
    #         MRStep(mapper=self.mapper, reducer=self.reducer_count),
    #         MRStep(reducer=self.reducer_max)
    #     ]

if __name__ == '__main__':
    MapReduce.run()