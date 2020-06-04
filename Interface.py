import pymongo
from pymongo import MongoClient
import pprint
import redis
import time

client = MongoClient('localhost', 27017)
redisClient = redisClient = redis.Redis(host="localhost",port=6379)
db = client.Netflix

titles = db.Title
types = db.Type
cast = db.Cast
info = db.Info
db.list_collection_names()
[u'titles']
[u'types']
[u'cast']
[u'info']

search_option = 0


def browser(collection, input, category, number):
    if number ==  True:
        mSearch = int(input)
    else:
        mSearch = input
    if not redisClient.exists(collection+input):
        result = titles.find_one({category: mSearch})
        print("Adding to redis")
        redisClient.set("movies_"+input, str(result)) 
        redisClient.expire("movies_"+input, 300)  
    else:
        print("Extracted from cache")
        result = redisClient.get(collection+input).decode("UTF-8")
    print(result)

def searchForGroup(info_input, command, key, collection):
    if not redisClient.sismember(key, info_input):
        print("Not in the cache")
        for doc in (collection.aggregate(command)):
            redisClient.rpush(info_input, str(doc))
        redisClient.sadd(key, info_input)
        redisClient.expire(key, 300)
        redisClient.expire(info_input, 300)
    else:
        print("Extracted from the cache")
    for i in range(0, redisClient.llen(info_input)):
        print(redisClient.lindex(info_input, i))

def SearchStadistics(collection, input, category, number, mongoCollection, keyName):
    if number ==  True:
        mSearch = int(input)
    else:
        mSearch = input
    if not redisClient.exists(collection+input):
        result = mongoCollection.count({category: mSearch})
        print("Not in cache")
        print("Adding to cache")
        redisClient.set(keyName+input, str(result))
        redisClient.expire(keyName+input, 300)
    else:
        print("Extracted from cache")
        result = redisClient.get(collection+input).decode("UTF-8")
    print(result)


while search_option != 5:
    print("----------Instruction--------")
    print(" ")
    print("1. Search for a movie by movie's info")
    print("2. Search for a movies by extra info of the movie")
    print("3. obtain statistics about the database")
    print("4. add or update an entity in the database")
    print("5. Nevermind")
    print(" ")

    search_option = int(input("Select an option: "))
    if search_option == 1:
        search_movie = 0
        while search_movie != 4:
            print(" ")
            print("1. Search movie information by tittle")
            print("2. Search movie information by id")
            print("3. Search by description")
            print("4. exit option")
            print(" ")
            search_movie = int(input("Select an option: "))
            if search_movie == 1:
                titleName = input("Write the name of the title: ")
                browser("movies_", titleName, "title", False)
            elif search_movie == 2:
                show_id = int(input("Write the id of the movie: "))
                browser("movies_", str(show_id), "show_id", True)
            elif search_movie == 3:
                description = input("Write the description of the title: ")
                browser("movies_", description, "description", False)
    elif search_option == 2:
        option_input = 0
        while option_input != 4:
            print(" ")
            print("1. search by cast")
            print("2. search by info")
            print("3. search by type")
            print("4. exit option")
            print(" ")

            option_input = int(input("Select an option: "))
            if option_input == 1:
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
                        searchForGroup(director_name, command, "directors_movies", cast)
                        #https://pythontic.com/database/redis/list
                    elif cast_option == 2:
                        print("")
                        cast_name = input("Write the cast's name: ")
                        print("")
                        command = [{'$match':{"cast":cast_name}},{'$lookup':{'from':"Title",'localField':"show_id",
                        'foreignField':"show_id",'as':"movies"}},{'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,
                        "cast":1,"movies.title":1}}]
                        searchForGroup(cast_name, command, "cast_movies", cast)
            elif option_input == 2:
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
                    searchForGroup(info_input, command, key, info)
            elif option_input == 3:
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
                    searchForGroup(type_input, command, key, types)
    elif search_option == 3:
        option_stats = 0
        while option_stats != 5:
            print("1. Show how many TV Shows there are")
            print("2. The total number of mexican titles ")
            print("3. Total titles released in 2016")
            print("4. Get the maxmeory of the cache")
            print("5. Exit option")
            option_stats = (int)(input("Select an option: "))
            if option_stats == 1:
                key_q = "TV Show"
                print("This is the total number of TV Shows")
                SearchStadistics("Tvshows_", key_q, "type", False, types, "Tvshows_")
            elif option_stats == 2:
                key_q = "Mexico"; 
                print("The total number of mexican titles")
                SearchStadistics("MexTitles_", key_q, "country", False, info, "MexTitles_")
            elif option_stats == 3:
                key_q = "2016";  
                print("Titles released in 2016")
                SearchStadistics("ReleaseTitle_", key_q, "release_year", False, info, "ReleaseTitle_")
            elif option_stats == 4:
                print(redisClient.config_get('maxmemory'))
    elif search_option == 4:
        print("")
        print("This will erase all the data from the cache, will you like to continue...?")
        print("1. Yes")
        print("2. No")
        decision = 0
        decision = int(input("Select an option: "))
        if decision == 1:
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

redisClient.close()
client.close()
