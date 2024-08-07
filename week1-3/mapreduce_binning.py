from mrjob.job import MRJob

class MapReduce(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(",")
            datetime = data[2].strip()
            year = datetime.split(' ')[0].split('/')[2]
            status_type = data[1].strip()
            num_reaction = data[3].strip()
            # date = datetime.split(' ')[0].split('/')[0]

            if year == '2018' :
                if status_type == 'photo' :
                    yield 'Photo', data
                elif status_type == 'video' :
                    yield 'video', data
                elif status_type == 'link' :
                    yield 'link', data
                elif status_type == 'status' :
                    yield 'status', data
            
    # def reducer(self, key, values):
    #     yield key, values

if __name__ == '__main__':
    MapReduce.run()