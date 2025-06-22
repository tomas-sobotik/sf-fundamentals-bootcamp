--Create a new table for holding the JSON data
create table json_sample (value variant);

--running the insert statements
insert into json_sample
select parse_json('
{"id":1,"first_name":"Madelena","last_name":"Bastiman","email":"mbastiman0@washington.edu","gender":"Female","ip_address":"47.136.171.159","language":"Bislama","city":"Lagunas","street":"Nevada","street_number":"2","phone":"+51 224 307 3778"}');
insert into json_sample
select parse_json('
{"id":2,"first_name":"Jasmine","last_name":"Hayth","email":"jhayth1@soup.io","gender":"Female","ip_address":"44.117.102.69","language":"Papiamento","city":"Sikeshu","street":"Dovetail","street_number":"7922","phone":"+86 710 521 7096"}');
insert into json_sample
select parse_json('
{"id":3,"first_name":"Doria","last_name":"Brownjohn","email":"dbrownjohn2@unblog.fr","gender":"Female","ip_address":"242.221.58.251","language":"Greek","city":"Trollhättan","street":"Dayton","street_number":"8348","phone":"+46 207 829 2153"}');
insert into json_sample
select parse_json('
{"id":4,"first_name":"Gaylor","last_name":"Enderson","email":"genderson3@istockphoto.com","gender":"Bigender","ip_address":"169.138.17.143","language":"Swahili","city":"Velizh","street":"Sugar","street_number":"1","phone":"+7 659 246 7831"}');
insert into json_sample
select parse_json('
{"id":5,"first_name":"Gaile","last_name":"Elcombe","email":"gelcombe4@japanpost.jp","gender":"Male","ip_address":"13.24.168.205","language":"Haitian Creole","city":"L’vovskiy","street":"Surrey","street_number":"3751","phone":"+7 641 563 0389"}');
