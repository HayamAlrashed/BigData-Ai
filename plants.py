
from mrjob.job import MRJob
from mrjob.step import MRStep

class hadoop(MRJob):
    def steps(self):
        return[
            MRStep(
            mapper=self.mapper_names,
                reducer=self.reducer_names
            )
#             ,
#                         MRStep(
#             mapper=self.mapper_names2,
#                 reducer=self.reducer_names2
#             )
        ]
    def mapper_names(self,_,line):
        (STATION_NAME,OBSERVATION_DATE,ELEVATION,WIND_DIRECTION_ANGLE,WIND_TYPE,WIND_SPEED_RATE,SKY_CEILING_HEIGHT,
        SKY_CAVOK,VISIBILITY_DISTANCE,AIR_TEMPERATURE,Crop,TEMP_MAX,TEMP_MIN,Crop_encoded) = line.split(",")
        yield (STATION_NAME,1)
        
    def reducer_names (self,keys,values):
        yield (keys,sum(values))
        
#     def mapper_names2(self,keys,values):
#         (bookID,average_rating) = keys
#         yield (bookID,(int(average_rating),values))
        
#     def reducer_names2 (self,key2,values2):
#         yield (key2,sum(int(values2)))    
    
    
        
if __name__ == "__main__":
    hadoop.run()
