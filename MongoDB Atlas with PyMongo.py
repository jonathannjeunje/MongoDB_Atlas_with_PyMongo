# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Project MongoDB

# %%
import pymongo
from pymongo import MongoClient
import pprint # for pretty printing

# %% [markdown]
# ## Credentials:
# 

# %%
username = "Jonalex210"
password = "6u3LvaTUPrqz9NL"

# %% [markdown]
# ## Establish connection to the database:

# %%
client = pymongo.MongoClient("mongodb+srv://"+username+":"+password+"@jonathannjeunje-cluster.cgfhy.mongodb.net/WarAndPeaceAnalytics?retryWrites=true&w=majority")
db = client.WarAndPeaceAnalytics
print(db)

# %% [markdown]
# ## Set the current collections:

# %%
myColl1 = db.nameLocations
myColl2 = db.paragraphNames

# %% [markdown]
# ## Clean the collections:
# Delete all documents.

# %%
delColl = myColl1.delete_many({}); print(delColl.deleted_count, " documents deleted.")
delColl = myColl2.delete_many({}); print(delColl.deleted_count, " documents deleted.")

# %% [markdown]
# ## Populate the collections:
# Open the file "WarAndPeace_Altered.txt" (same folder) and read it line by line
# looking for the location of the namaes in each paragraph
# 
# Every time you find a name from the list, create a document that will record the name, the line (paragraph) number and the index (location) of the name within that paragraph.
# 
# For example, if you find a name “Natasha” in paragraph (line in the file) 445 at location 161 of that line, then you create the following document.
# 
# {
#    "_id" : ObjectId("5b975c49430e68169c39bd6f"),
#    "name" : "Natasha",
#    "paragraph" : 445,
#    "parIndex" : 161
# }
# 
# Moreover, if you find the name “Natasha” in the same paragraph again you will record it as a separate document. For example:
# 
# {
#    "_id" : ObjectId("5b975c49430e68169c39bd70"),
#    "name" : "Natasha",
#    "paragraph" : 445,
#    "parIndex" : 327
# }
# 
# Do this for all the characters provided in our list.
# Use your program that extracts names from the file and populate another collection: “paragraphNames” with the following documents:
# 
# Every time you find a paragraph with at least two names from the list in it, create a document that will record the name, the line (paragraph) number and  the array of names found in this paragraph.
# 
# For example, if you find a name “Natasha” and “Prince Andrew” in the same paragraph (say, in line 999), and no other names in the list, then you create the following document.
# 
# {
#    "_id" : ObjectId("5b9a267a6454418c889b7dee"),
#    "paragraph" : 999,
#    "names" : [“Natasha”, “Prince Andrew”]
# }
# 
# Do this for all paragraphs that contain at least two names. (Alternatively, populate “paragraphNames” using aggregation pipeline from ‘nameLocations’.

# %%
filepath = 'WarAndPeace_Altered.txt'

with open(filepath) as fp: # if you are using Linux or Mac, you may need to use decode() function.

    line = fp.readline() # Reads the first line
    cntPar = 0 # Counter for all encountered paragraphs
    cntLines = 0
    coll1Docs = []
    coll2Docs = []
    
    while line:
        if len(line) > 1: # Check if this line is not empty (i.e. has no text). 
            hits = []
            names = {"Natasha", "Pierre", "Denisov", "Nicholas", "Countess Mary", "Kutuzov", "Bolkonski", "Napoleon", "Prince Andrew", "Dolokhov", "Anna Pavlovna", "Helene", "Princess Mary", "Prince Vasili", "Bezukhov", "Boris",  "Sonya", "Rostovs", "Anatole"}
        
            for name in names:
                index = line.find(name) # Find fist occurance of the name in line.
                
                if index >= 0: hits.append(name)
                initIndex = 0
                while index >= 0: # A hit occured & Looping for more occurances.
                    index = index + initIndex
                    
                    myDoc = { "name": name, "paragraph": cntPar, "parIndex": index }; #print(myDoc)
                    coll1Docs.append(myDoc)
                    
                    # insColl = myColl1.insert_one(myDoc) # here myColl_... is a collection in a current database
                    # print(ins_coll.inserted_id)
                    
                    initIndex = index + len(name)
                    index = line[initIndex:].find(name)
                                 
            if len(hits) >= 2: # for all paragraphs that contain at least two names.
                myDoc = { "paragraph": cntPar, "names": hits }; #print(myDoc)
                coll2Docs.append(myDoc)
                
                # insColl = myColl2.insert_one(myDoc) # here myColl_... is a collection in a current database
                # print(ins_coll.inserted_id)
            
            cntPar += 1
            
        line = fp.readline()
        cntLines += 1
        
    print("\nTotal # of Lines: {}".format(cntLines))
    print("Total # of paragraphs: {}".format(cntPar))
    #print(len(coll1Docs))
    #print(len(coll2Docs))


# %%
ids = myColl1.insert_many(coll1Docs)
ids = myColl2.insert_many(coll2Docs)

# %% [markdown]
# ## Queries:
# %% [markdown]
# ### Query 1:
# Create a query for MongoDB to see how many times two given characters are mentioned within the same paragraphs. For example, how many times are “Natasha” and "Prince Andrew" mentioned within the same paragraphs? (In other words, count the number of paragraphs that mention both “Natasha” and “Prince Andrew”.)
# 
# Make two queries: one that works on ‘nameLocations’ collection and the other one that works on ‘paragraphNames’ collection. Picking any two characters for your examples (I should be able to replace those with any other pair of characters and the query should still work).

# %%
name1 = "Natasha"
name2 = "Prince Andrew"

# %% [markdown]
# #### Collection: nameLocations

# %%
pipeline = [
    {
        '$group': {
            '_id': '$paragraph', 
            'names': {
                '$addToSet': '$name'
            }
        }
    }, {
        '$match': {
            'names': {
                '$all': [
                    name1, name2
                ]
            }
        }
    }, {
        '$count': 'count'
    }
]

result = (myColl1.aggregate(pipeline))
pprint.pprint(list(result))

# %% [markdown]
# #### Collection: paragraphNames

# %%
myColl2.count_documents({"names":{"$all":[name1,name2]}})

# %% [markdown]
# Or

# %%
pipeline = [
    {
        '$match': {
            'names': {
                '$all': [
                    'Natasha', 'Prince Andrew'
                ]
            }
        }
    }, {
        '$count': 'count'
    }
]

result = (myColl2.aggregate(pipeline))
pprint.pprint(list(result))

# %% [markdown]
# ### Query 2:
# 
# Create a query for MongoDB that, given an ordered pair of two characters, will count how many times is the first character mentioned within the same or neighboring paragraphs as the second character. Here, 'neighboring' means right before or right after.  Make this query work on any collection you want.
# 
#  
# 
# Let me clarify.
# 
#  
# 
# Suppose we are considering Natasha and Sonya (i.e. Natasha is first and Sonya second). 
# 
#  
# 
# For every paragraph where Natasha is mentioned, consider the following:
# 
# - Is Sonya mentioned within the same paragraph?
# 
# - Is Sonya mentioned in the previous paragraph?
# 
# - Is Sonya mentioned in the next paragraph? 
# 
# If the answer is yes, to any of these, then we add 1 to our count (but not more than 1, even if all answers are yes). Move to the next paragraph where Natasha is mentioned.
# 
#  
# 
# This is not necessarily an algorithm to follow, just an explanation on what I mean by this count.
# 
#  
# 
# Notice that counts for (Natasha, Sonya) can be different to (Sonya, Natasha), so it is important to follow the order, who is the first character and who is the second

# %%
name1 = "Natasha"
name2 = "Sonya"


# %%
pipeline = [
    {
        '$group': {
            '_id': '$paragraph', 
            'atNames': {
                '$addToSet': '$name'
            }
        }
    }, {
        '$match': {
            'atNames': {
                '$all': [
                    name1
                ]
            }
        }
    }, {
        '$project': {
            'atNames': 1, 
            'bePar': {
                '$add': [
                    '$_id', -1
                ]
            }, 
            'afPar': {
                '$add': [
                    '$_id', 1
                ]
            }
        }
    }, {
        '$lookup': {
            'from': 'paragraphNames1', 
            'localField': 'bePar', 
            'foreignField': '_id', 
            'as': 'from_bePar'
        }
    }, {
        '$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    {
                        '$arrayElemAt': [
                            '$from_bePar', 0
                        ]
                    }, '$$ROOT'
                ]
            }
        }
    }, {
        '$project': {
            'beNames': '$names', 
            'atNames': 1, 
            'afPar': 1
        }
    }, {
        '$lookup': {
            'from': 'paragraphNames1', 
            'localField': 'afPar', 
            'foreignField': '_id', 
            'as': 'from_afPar'
        }
    }, {
        '$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    {
                        '$arrayElemAt': [
                            '$from_afPar', 0
                        ]
                    }, '$$ROOT'
                ]
            }
        }
    }, {
        '$project': {
            'afNames': '$names', 
            'atNames': 1, 
            'beNames': 1
        }
    }, {
        '$addFields': {
            'beNames': {
                '$cond': {
                    'if': {
                        '$ne': [
                            {
                                '$type': '$beNames'
                            }, 'array'
                        ]
                    }, 
                    'then': [], 
                    'else': '$beNames'
                }
            }, 
            'afNames': {
                '$cond': {
                    'if': {
                        '$ne': [
                            {
                                '$type': '$afNames'
                            }, 'array'
                        ]
                    }, 
                    'then': [], 
                    'else': '$afNames'
                }
            }
        }
    }, {
        '$project': {
            'beMent': {
                '$cond': [
                    {
                        '$in': [
                            name2, '$beNames'
                        ]
                    }, 1, 0
                ]
            }, 
            'atMent': {
                '$cond': [
                    {
                        '$in': [
                            name2, '$atNames'
                        ]
                    }, 1, 0
                ]
            }, 
            'afMent': {
                '$cond': [
                    {
                        '$in': [
                            name2, '$afNames'
                        ]
                    }, 1, 0
                ]
            }
        }
    }, {
        '$match': {
            '$or': [
                {
                    'beMent': {
                        '$ne': 0
                    }
                }, {
                    'atMent': {
                        '$ne': 0
                    }
                }, {
                    'afMent': {
                        '$ne': 0
                    }
                }
            ]
        }
    }, {
        '$count': 'count'
    }
]

result = (myColl1.aggregate(pipeline))
pprint.pprint(list(result))

# %% [markdown]
# ### Query 3:
# 
# Create a query for MongoDB to see who is mentioned the most in the book (total number of times somebody is mentioned, counting multiple times per paragraph, if needed).

# %%
pipeline = [
    {
        '$group': {
            '_id': '$name', 
            'nMentioned': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'nMentioned': -1
        }
    }, {
        '$limit': 1
    }
]

result = (myColl1.aggregate(pipeline))
pprint.pprint(list(result))

# %% [markdown]
# ### Query 4:
# 
# Create a query for MongoDB to see who is mentioned in the largest number of paragraphs.

# %%
pipeline = [
    {
        '$group': {
            '_id': '$name', 
            'parMentioned': {
                '$addToSet': '$paragraph'
            }
        }
    }, {
        '$project': {
            'nParMentioned': {
                '$max': {
                    '$size': '$parMentioned'
                }
            }
        }
    }, {
        '$sort': {
            'nParMentioned': -1
        }
    }, {
        '$limit': 1
    }
]

result = (myColl1.aggregate(pipeline))
pprint.pprint(list(result))


# %%



# %%



