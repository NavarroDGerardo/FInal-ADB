import pymongo
from pymongo import MongoClient
import pprint
import redis

client = MongoClient('localhost', 27017)
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

#for title in titles.find():
    #pprint.pprint(title)
#Redis connection

#API for queries
option_1 = 0
option_2 = 0
option_3 = 0

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
                #pregutarle al cache y si no esta llenar la data en el cache
                pprint.pprint(titles.find_one({"title": titleName}))
            elif option_2 == 2:
                show_id = int(input("Write the id of the movie: "))
                pprint.pprint(titles.find_one({"show_id": show_id}))
            elif option_2 == 3:
                description = input("Write the description of the title: ")
                pprint.pprint(titles.find_one({"description": description}))
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
                #director
                #cast
                print(" ")
                print("1. Search titles by director")
                print("2. Search titles by cast")
                print("3. exit option")
                print(" ")

                cast_option = (int)(input("Select an option: "))
                command = ""
                if cast_option == 1:
                    print("")
                    director_name = input("Write the director's name: ")
                    print("")
                    command = [{'$match':{"director":director_name}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"director":1,"movies.title":1}}]
                elif cast_option == 2:
                    print("")
                    cast_name = input("Write the cast's name: ")
                    print("")
                    command = [{'$match':{"cast":cast_name}},{'$lookup':{'from':"Title",'localField':"show_id",
                'foreignField':"show_id",'as':"movies"}},{'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,
                "cast":1,"movies.title":1}}]
                if command != "":
                  for doc in (cast.aggregate(command)):
                    pprint.pprint(doc)
            elif option_3 == 2:
                #country
                #date_added
                #release_year
                #duration
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
                    country_name = input("Enter the country: ")
                    print("")
                    command = [{'$match':{"country":country_name}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"country":1,"movies.title":1}}]
                elif info_result == 2:
                    print("")
                    date_a = input("Enter the date added: ")
                    print("")
                    command = [{'$match':{"date_added":date_a}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"date_added":1,"movies.title":1}}]
                elif info_result == 3:
                    print("")
                    year_res = input("Enter the realease year: ")
                    print("")
                    command = [{'$match':{"release_year":year_res}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"release_year":1,"movies.title":1}}]
                elif info_result == 4:
                    print("")
                    duration_res = input("Enter the duration: ")
                    print("")
                    command = [{'$match':{"duration":duration_res}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"duration":1,"movies.title":1}}]

                if command != "":
                    for doc in (info.aggregate(command)):
                        pprint.pprint(doc)
            elif option_3 == 3:
                #type
                #rating
                #listed_in
                print("")
                print("1. Search by type")
                print("2. Search by rating")
                print("3. search by listed in")
                print("4. exit option")
                print("")

                category = int(input("Select an option: "))
                if category == 1:
                    print("")
                    print("1. Serch by movie")
                    print("2. Search by TV show")
                    print(" ")
                    
                    t_type = int(input("Select an option: "))
                    tp = ""
                    if t_type == 1:
                        tp = "Movie"  
                    if t_type == 2:
                        tp = "TV Show"
                    
                    command = [{'$match':{"type":tp}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,"movies.title":1}},{'$limit':10}]
                    for doc in (types.aggregate(command)):
                        pprint.pprint(doc)
                elif category == 2:
                    print("")
                    print("1. TV-PG")
                    print("2. TV-MA")
                    print("3. TV-Y7-FV")
                    print("3. TV-Y7")
                    print("4. TV-14")
                    print("5. R")
                    print("6. TV-Y")
                    print("7. NR")
                    print("8. PG-13")
                    print("9. TV-G")
                    print("10. PG")
                    print("11. G")
                    print("12. ")
                    print("13. UR")
                    print("14. NC-17")
                    print("")

                    ra = (int)(input("Select an option: "))
                    ra_response = ""
                    if ra == 1:
                        ra_response = "TV-PG"  
                    elif ra == 2:
                        ra_response = "TV-MA"
                    elif ra == 3:
                         ra_response = "TV-Y7-FV"
                    elif ra == 4:
                         ra_response = "TV-Y7"  
                    elif ra == 5:
                         ra_response = "R"  
                    elif ra == 6:
                         ra_response = "TV-Y" 
                    elif ra == 7:
                         ra_response = "NR" 
                    elif ra == 8:
                         ra_response = "PG-13" 
                    elif ra == 9:
                         ra_response = "TV-G" 
                    elif ra == 10:
                         ra_response = "PG"
                    elif ra == 11:
                         ra_response = "G"  
                    elif ra == 12:
                         ra_response = ""  
                    elif ra == 13:
                         ra_response = "UR"  
                    elif ra == 14:
                         ra_response = "NC-17"    
                    #falta poner elif de los demás
                    command = [{'$match':{"rating":ra_response}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,"movies.title":1}},{'$limit':10}]
                    for doc in (types.aggregate(command)):
                        pprint.pprint(doc)
                elif category == 3:
                    print("")
                    l_in = input("Write the category listed in of the movie: ")
                    print("")
                    command = [{'$match':{"listed_in":l_in}},{'$lookup':{'from':"Title",'localField':"show_id",'foreignField':"show_id",'as':"movies"}},
                    {'$unwind':"$movies"},{'$project':{'_id':0,"show_id":1,"movies.title":1}},{'$limit':10}]
                    for doc in (types.aggregate(command)):
                        pprint.pprint(doc)

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
        cast_id = cast.insert_one(new_cast).inserted_id

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

