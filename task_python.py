import psycopg2
from psycopg2 import OperationalError
import json
import sys


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

class Executor:
    def __init__(self, connection):
        self.connection = connection
    def execute_query(self,query):
        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(query)
        except OperationalError as e:
            print(f"The error '{e}' occurred")


    def execute_select_query(self,query):
        cursor = self.connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"The error '{e}' occurred")


create_table = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))
create_table.execute_query("""
CREATE TABLE IF NOT EXISTS rooms(
id INTEGER PRIMARY KEY,
name TEXT NOT NULL)""")
create_table.execute_query("""
CREATE TABLE IF NOT EXISTS students(
  birthday TIMESTAMP, 
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL, 
  room INTEGER REFERENCES rooms(id),
  sex TEXT NOT NULL)""")

class File_manager:
    def read(self, path):
        try:
            file = json.loads(open(path).read())
            return file
        except IOError as e:
            print(e)

    def new_write(self, path, content):
        try:
            with open(path, 'w') as outfile:
                json.dump(content, outfile)
        except IOError as e:
            print(e)


    def write(self, path, content):
        try:
            with open(path, 'a') as outfile:
                json.dump(content, outfile)
        except IOError as e:
            print(e)

myfile_rooms = File_manager()
rooms = myfile_rooms.read(input('Please choose path for file rooms'))

rooms_writer = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))

for item in rooms:
    room_id = item.get('id')
    room_name = item.get('name')
    rooms_writer.execute_query(f"insert into rooms(id,name) values({room_id}, '{room_name}')")

myfile_students = File_manager()
students = myfile_students.read(input('Please choose path for file students'))

students_writer = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))

for item in students:
    birthday = item.get('birthday')
    id = item.get('id')
    name = item.get('name')
    room = item.get('room')
    sex = item.get('sex')
    students_writer.execute_query(f"insert into students(birthday, id, name, room, sex) values('{birthday}', {id}, '{name}',{room},'{sex}')")

indexes = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))
indexes.execute_query("""CREATE INDEX IF NOT EXISTS student_room_index
ON public.students(room)""")


rooms_with_students = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))
data_rooms_with_students = rooms_with_students.execute_select_query("""SELECT rooms.name, COUNT(students.name) as Number_of_students
FROM public.rooms
INNER JOIN public.students ON(rooms.id = students.room)
GROUP BY rooms.name
ORDER BY rooms.name""")

select1_data = []
for room in data_rooms_with_students:
    select1_data.append({room[0]: room[1]})
select1 = File_manager()
path = input('Please choose the full path for saving your results in json')
select1_file = select1.new_write(path, select1_data)

rooms_with_rank = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))
data_rooms_with_rank = rooms_with_rank.execute_select_query("""WITH t2 as (
SELECT rooms.name, 
(SUM(DATE_PART('year', CURRENT_DATE::date) - DATE_PART('year', students.birthday::date)) / COUNT(students.name)) as average_age
FROM public.rooms
INNER JOIN public.students ON(rooms.id = students.room)
GROUP BY rooms.name)
SELECT name, place FROM (SELECT name, ROW_NUMBER() OVER(ORDER BY average_age) as place
FROM t2) t1
WHERE place <6""")

select2_data = []
for room in data_rooms_with_rank:
    select2_data.append({room[0]: room[1]})
select2 = File_manager()
select2_file = select2.write(path, select2_data)

rooms_with_rank_age = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))
data_rooms_with_rank_age = rooms_with_rank_age.execute_select_query("""WITH t2 as (SELECT rooms.name, 
MAX(DATE_PART('year', CURRENT_DATE::date) - DATE_PART('year', students.birthday::date)) - 
MIN (DATE_PART('year', CURRENT_DATE::date) - DATE_PART('year', students.birthday::date)) as age_gap
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
GROUP BY rooms.name)
SELECT name, place FROM (SELECT name, ROW_NUMBER() OVER(ORDER BY age_gap DESC) as place
FROM t2) t1
WHERE place < 6""")

select3_data = []
for room in data_rooms_with_rank_age:
    select3_data.append({room[0]: room[1]})
select3 = File_manager()
select3_file = select3.write(path, select3_data)

rooms_with_male_female = Executor(create_connection("test_db", "postgres", "postgre", "127.0.0.1", "5432"))
data_rooms_with_male_female = rooms_with_male_female.execute_select_query("""SELECT rooms.name FROM rooms WHERE rooms.name IN 
(SELECT rooms.name
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
WHERE students.sex != 'F'
GROUP BY rooms.name) AND rooms.name  
IN (SELECT rooms.name
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
WHERE students.sex != 'M'
GROUP BY rooms.name)""")

select4_data = []
for room in data_rooms_with_male_female:
    select4_data.append(room[0])
select4 = File_manager()
select4_file = select4.write(path, select4_data)

