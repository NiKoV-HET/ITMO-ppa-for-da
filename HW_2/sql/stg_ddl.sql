create table stg.work_programs
(id integer, academic_plan_in_field_of_study text, wp_in_academic_plan text, update_ts timestamp);
create table stg.su_wp
(fak_id integer, fak_title text, wp_list text);
create table stg.up_detail
(id integer, ap_isu_id integer, on_check varchar(20), laboriousness integer, academic_plan_in_field_of_study text);
create table stg.wp_detail
(id integer, discipline_code varchar(20), title text, description text, structural_unit varchar(100), prerequisites text, discipline_sections text, bibliographic_reference text, outcomes text, certification_evaluation_tools text, expertise_status varchar(3));
