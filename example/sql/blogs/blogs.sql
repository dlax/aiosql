-- name: get_all_blogs
select * from blogs;

-- name: publish_blog<!
insert into blogs(userid, title, content) values (:userid, :title, :content);

-- name: insert_many*!
insert into blogs(userid, title, content, published) values (?, ?, ?, ?);


-- name: get_user_blogs
-- record_class: UserBlog
-- Get blogs with a fancy formatted published date and author field
    select b.blogid,
           b.title,
           strftime('%Y-%m-%d %H:%M', b.published) as published,
           u.username as author
      from blogs b
inner join users u on b.userid = u.userid
     where u.username = :username
  order by b.published desc;
