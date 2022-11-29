EXPLAIN ANALYZE 
SELECT * from students
--список комнат и количество студентов в каждой из них

SELECT rooms.name, COUNT(students.name) as Number_of_students
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
GROUP BY rooms.name
ORDER BY rooms.name

--top 5 комнат, где самый маленький средний возраст студентов
WITH t2 as (
SELECT rooms.name, 
(SUM(DATE_PART('year', CURRENT_DATE::date) - DATE_PART('year', students.birthday::date)) / COUNT(students.name)) as average_age
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
GROUP BY rooms.name)
SELECT name, place FROM (SELECT name, ROW_NUMBER() OVER(ORDER BY average_age) as place
FROM t2) t1
WHERE place <6

--top 5 комнат с самой большой разницей в возрасте студентов


WITH t2 as (SELECT rooms.name, 
MAX(DATE_PART('year', CURRENT_DATE::date) - DATE_PART('year', students.birthday::date)) - 
MIN (DATE_PART('year', CURRENT_DATE::date) - DATE_PART('year', students.birthday::date)) as age_gap
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
GROUP BY rooms.name)
SELECT name, place FROM (SELECT name, ROW_NUMBER() OVER(ORDER BY age_gap DESC) as place
FROM t2) t1
WHERE place < 6


--список комнат где живут разнополые студенты.
SELECT rooms.name FROM rooms WHERE rooms.name IN 
(SELECT rooms.name
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
WHERE students.sex != 'F'
GROUP BY rooms.name) AND rooms.name  
IN (SELECT rooms.name
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
WHERE students.sex != 'M'
GROUP BY rooms.name)

/*EXPLAIN ANALYZE SELECT rooms.name, students.name
FROM rooms
INNER JOIN students ON(rooms.id = students.room)
WHERE rooms.name = 'Room #345'
ORDER BY rooms.name;*/

/*индекс по таблице которая часто джойнится и данные хранятся не в отсортированном виде. Праймари индексы уже есть
Создание индекса-B-дерева по столбцу room в таблице students*/
/*CREATE UNIQUE INDEX student_room_index
ON students(room)*/
