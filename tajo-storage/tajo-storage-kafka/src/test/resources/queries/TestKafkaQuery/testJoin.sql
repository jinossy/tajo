select u.id, u.name, p.id, p.prod_name, p.point from user u join prod p on(u.id = p.id) order by u.id