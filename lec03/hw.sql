/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

select c.name         as category_name,
       count(film_id) as films_in_category
from film_category fc
         left join category c on fc.category_id = c.category_id
group by c.name;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

select a.first_name || ',' || a.last_name as actor_name,
       count(r.rental_id)                 as most_rentals
from actor a
         join film_actor fa on a.actor_id = fa.actor_id
         join inventory i on fa.film_id = i.film_id
         join rental r on i.inventory_id = r.inventory_id
group by a.actor_id, a.first_name, a.last_name
order by most_rentals desc
limit 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

select c.name        as category_name,
       sum(p.amount) as total_spent
from payment p
         join rental r on p.rental_id = r.rental_id
         join inventory i on r.inventory_id = i.inventory_id
         join film_category fc on i.film_id = fc.film_id
         join category c on fc.category_id = c.category_id
group by c.category_id, c.name
order by total_spent desc
limit 1;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

select f.title as not_exists_in_inventory
from film f
         left join inventory i on f.film_id = i.film_id
where i.film_id is null;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

select a.first_name || ',' || a.last_name as actor_name,
       count(*)                           as with_children
from actor a
         join film_actor fa on a.actor_id = fa.actor_id
         join film_category fc on fa.film_id = fc.film_id
         join category c on fc.category_id = c.category_id
where c.name = 'Children'
group by a.actor_id, a.first_name, a.last_name
order by with_children desc
limit 3;
