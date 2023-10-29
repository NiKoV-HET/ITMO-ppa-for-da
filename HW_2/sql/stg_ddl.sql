create table stg.work_programs
(id integer, academic_plan_in_field_of_study text, wp_in_academic_plan text, update_ts timestamp);
create table stg.su_wp
(fak_id integer, fak_title text, wp_list text, update_ts timestamp);
create table stg.up_detail
(id integer, ap_isu_id integer, on_check varchar(20), laboriousness integer, academic_plan_in_field_of_study text, update_ts timestamp);
create table stg.wp_detail
(id integer, discipline_code varchar(20), title text, description text, status varchar(3), update_ts timestamp);
