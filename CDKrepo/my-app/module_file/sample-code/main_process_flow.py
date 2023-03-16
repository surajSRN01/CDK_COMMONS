from utilities import mongo_utils as m
from utilities import dynamo_utils as d
from utilities import transform as transform



def main(bucket_name,db):
    pdf = transform.trans(bucket_name)
    saveToDB = False
    if (db=="MONGODB"):
        saveToDB = m.save_to_mongo(pdf)
    elif (db=="DYNAMODB"):
        saveToDB = d.save_to_dynamo(pdf)
    else:
        saveToDB = False
    if(saveToDB):
        print("Data saved Successfully.. \n Thank You Please save again!")
    else:
        print("Sorry Having Some Issue with You!")

    
        
   

