update grouper_ddl set last_updated = to_char(systimestamp, 'YYYY/MM/DD HH12:MI:SS'), history = substr((to_char(systimestamp, 'YYYY/MM/DD HH12:MI:SS') || ': upgrade Grouper from V' || db_version || ' to V47, ' || history), 1, 3500), db_version = 47 where object_name = 'Grouper';
commit;
