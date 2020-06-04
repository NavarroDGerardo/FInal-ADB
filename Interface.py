import pymongo
from pymongo import MongoClient
import pprint
import redis
import time


#Intitializing the clients 
client = MongoClient('localhost', 27017)
redisClient = redisClient = redis.Redis(host="localhost",port=6379)
db = client.Netflix

#Definition Of tables
titles = db.Title
types = db.Type
cast = db.Cast
info = db.Info
db.list_collection_names()
[u'titles']
[u'types']
[u'cast']
[u'info']

option_1 = 0
option_2 = 0
option_3 = 0


def browser(collection, input, category, number):
    if number ==  True:
        mSearch = int(input)
    else:
        mSearch = input
    if not redisClient.exists(collection+input):
        result = titles.find_one({category: mSearch})
        print("Adding to redis")
        redisClient.set("movies_"+input, str(result))   
    else:
        print("Extracted from cache")
        result = redisClient.get(collection+input).decode("UTF-8")
    print(result)

def searchForGroup(director_name, command, key):
    if not redisClient.sismember(key, director_name):
        print("Not in the cache")
        for doc in (cast.aggregate(command)):
            redisClient.rpush(director_name, str(doc))
        redisClient.sadd(key, director_name)
    else:
        print("Extracted from the cache")
    for i in range(0, redisClient.llen(director_name)):
        print(redisClient.lindex(director_name, i))

while option_1 != 5:
    print("----------Instruction--------")
    print(" ")
    print("1. Search for a specific data")
    print("2. Search for a group")
    print("3. obtain statistics about the database")
    print("4. add or update an entity in the database")
    print("5. Nevermind")
    print(" ")

    option_1 = int(input("Select an option: "))
    if option_1 == 1:
        while option_2 != 4:
            print(" ")
            print("1. Search movie information by tittle")
            print("2. Search movie information by id")
            print("3. Search by description")
            print("4. exit option")
            print(" ")
            option_2 = int(input("Select an option: "))
            if option_2 == 1:
                titleName = input("Write the name of the title: ")
                browser("movies_", titleName, "title", False)
            elif option_2 == 2:
                show_id = int(input("Write the id of the movie: "))
                browser("movies_", str(show_id), "show_id", True)
            elif option_2 == 3:
                description = input("Write the description of the title: ")
                browser("movies_", description, "description", False)
    elif option_1 == 2:
        while option_3 != 4:
            print(" ")
            print("1. search by cast")
            print("2. search by info")
            print("3. search by type")
            print("4. exit option")
            print(" ")

            option_3 = int(input("Select an option: "))
            if option_3 == 1:
                cast_option = 0
                while cast_option >= 0 and cast_option < 3:
                    print(" ")
                    print("1. Search titles by director")
                    print("2. Search titles by cast")
                    print("3. exit option")
                    print(" ")

                    cast_option = int(input("Select an option: "))
                    command = ""
                    if cast_option == 1:
                        print("")
                        director_name = input("Write the director's name: ")
                        print("")
                        command = [{'$match':{"director":director_name}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                            {'$unwind':"$movies"},{'$project':{'_id':0,"director":1,"movies.title":1}}]
                        searchForGroup(director_name, command, "directors_movies")
                        #https://pythontic.com/database/redis/list
                    elif cast_option == 2:
                        print("")
                        cast_name = input("Write the cast's name: ")
                        print("")
                        command = [{'$match':{"cast":cast_name}},{'$lookup':{'from':"Title",'localField':"show_id",
                        'foreignField':"show_id",'as':"movies"}},{'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,
                        "cast":1,"movies.title":1}}]
                        searchForGroup(cast_name, command, "cast_movies")
            elif option_3 == 2:
                info_result = 0
                while info_result >= 0 and info_result < 5:
                    print("")
                    print("1. Search by country")
                    print("2. Search by date added")
                    print("3. Search by realease year")
                    print("4. Search by duration")
                    print("5. exit option")
                    print("")
                    
                    info_result = (int)(input("Select an option: "))
                    command = ""
                    if info_result == 1:
                        print("")
                        info_input = input("Enter the country: ")
                        print("")
                        command = [{'$match':{"country":info_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"country":1,"movies.title":1}}]
                        key = "country_movies"
                    elif info_result == 2:
                        print("")
                        info_input = input("Enter the date added: ")
                        print("")
                        command = [{'$match':{"date_added":info_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"date_added":1,"movies.title":1}}]
                        key = "date_added_movies"
                    elif info_result == 3:
                        print("")
                        info_input = input("Enter the realease year: ")
                        print("")
                        command = [{'$match':{"release_year":info_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"release_year":1,"movies.title":1}}]
                        key = "year_movies"
                    elif info_result == 4:
                        print("")
                        info_input = input("Enter the duration: ")
                        print("")
                        command = [{'$match':{"duration":info_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"duration":1,"movies.title":1}}]
                        key = "duration_movies"
                    searchForGroup(info_input, command, key)
            elif option_3 == 3:
                category = 0
                while category >= 0 and category < 4:
                    print("")
                    print("1. Search by type")
                    print("2. Search by rating")
                    print("3. search by listed in")
                    print("4. exit option")
                    print("")
                    category = int(input("Select an option: "))
                    if category == 1:
                        print("")
                        type_input = input("Enter the type: ")
                        command = [{'$match':{"type":type_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,"movies.title":1}}]
                        key = "type_movies"
                    elif category == 2:
                        print("")
                        type_input = input("Write the rating of the movie: ")
                        command = [{'$match':{"rating":type_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,"movies.title":1}},{'$limit':10}]
                        key = "rating_movies"
                    elif category == 3:
                        print("")
                        type_input = input("Write the category listed in of the movie: ")
                        command = [{'$match':{"listed_in":type_input}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                        {'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,"movies.title":1}},{'$limit':10}]
                        key = "listed_movies"
                    searchForGroup(type_input, command, key)
    elif option_1 == 3:
         #Three Queries to obtain statistics about the database
        print("1. Show how many TV Shows there are")
        print("2. ")
        print("3. ")

        qa = (int)(input("Select an option: "))
        if qa == 1:
        #Query to obtain how many TV Shows are in the database
          print("This is the total number of TV Shows")
          pprint.pprint(types.find({"type": "TV Show"}).count()) 
        if qa == 2:
            print("Todavia en construccion")
        if qa == 3:

            print("Todavia en construccion")
   
         
    elif option_1 == 4:
        #One query to add or update an entity in the database
        print("")
        print("To insert a new title we need to add data in all the collections")
        print("")

        print("=======Title data=========")
        print("")
        sh_id = int(input("Enter the show id: "))
        title = input("Enter the title: ")
        description = input("Enter the description of the title")

        new_title = {"show_id": sh_id, "title": title,"description": description}
        title_id = titles.insert_one(new_title).inserted_id

        print(" ")
        print(title_id)
        print("")

        print("=======Info of the title data=========")
        print("")
        country_name = input("Enter the country: ")
        date_added = input("Enter the date added: ")
        release_y = input("Enter the release yer: ")
        duration = input("Enter the duration: ")

        new_info = {"show_id": sh_id, "country": country_name,"date_added": date_added, "release_year": release_y, "duration": duration}
        info_id = info.insert_one(new_info).inserted_id

        print(" ")
        print(info_id)
        print("")

        print("=======Cast of the title data=========")
        print("")
        director_n = input("Enter the director's name: ")
        cast_crew = input("Enter the  cast's name: ")

        new_cast = {"show_id": sh_id, "diretor":director_n, "cast":cast_crew}
        cast_id = cast.insert_one(new_cast).inserted_idvv

        print(" ")
        print(cast_id)
        print("")

        print("=======Type of the title data=========")
        print("")
        type_input = input("Enter the tyoe: ")
        rat_in = input("Enterrating: ")
        list_in = input("Enter the listed in: ")

        new_type = {"show_id": sh_id, "type":type_input, "rating":rat_in, "listed_in":list_in}
        type_id = types.insert_one(new_type).inserted_id

        print(" ")
        print(type_id)
        print("")
        
        #https://api.mongodb.com/python/current/tutorial.html

