# Credit-related-persons
Credit related persons


---------------
pragma include(::[INTEGR_FORMS].[LIB_MACRO_BR]);
pragma include(::[DEBUG_TRIGGER].[MACRO_LIB]);
-----временная таблица для сохранения связанных кредитов
type Tab_doc_id is record(
	t_doc_id		varchar2(100)
	,t_cr_cl		varchar2(100)
);
type Tab_doc_ids is table of Tab_doc_id;
v_Tab_doc_id	Tab_doc_ids;

------------------------проверка связанных кредитов на повтор и запись во временную таблицу
function v_check_repeat	(v_f_doc_id varchar2(100),v_f_cr_cl varchar2(100)) return number is
	retVal	number;
	ii 		integer;
	vIdx    number;
begin
	vIdx := v_Tab_doc_id.count + 1;
	retVal :=1;
	if v_Tab_doc_id.count = 0 then
		retVal :=1;
	else
		for ii in v_Tab_doc_id.first..v_Tab_doc_id.last where v_Tab_doc_id(ii).t_doc_id = v_f_doc_id and v_Tab_doc_id(ii).t_cr_cl = v_f_cr_cl
	    loop
	    	retVal :=0;
			exit;
		end loop;
		exit;
	end if;	
	if retVal = 1 then
	    v_Tab_doc_id(vIdx).t_doc_id := v_f_doc_id;
		v_Tab_doc_id(vIdx).t_cr_cl := v_f_cr_cl;
	end if;
	return retVal ;
end;

------------------------проверка связанных кредитов на повтор без записи в таблицу
function v_check_repeat_23(v_f_doc_id varchar2(100),v_f_cr_cl varchar2(100)) return number is
	retVal	number;
	ii 		integer;
begin
	retVal :=1;
	
	if v_Tab_doc_id.count = 0 then
		retVal :=1;
	else
		for ii in v_Tab_doc_id.first..v_Tab_doc_id.last where v_Tab_doc_id(ii).t_doc_id = v_f_doc_id and v_Tab_doc_id(ii).t_cr_cl = v_f_cr_cl
	    loop
	    	retVal :=0;
			exit;
		end loop;
	end if;	
	return retVal ;
end;

------------------------проверка остатка на кред траншах
function f_check_saldo (v_f_cred_doc_id number ,v_date date, v_f_cred_cr_sm_acc number ,v_f_v_summa number ,v_row_num number ) return number is
	retVal	number;
	data    date;
begin
	data := to_date(v_date);
	
	for (
		select crt(
			crt.[HIGH_LEVEL_CR]													    :crt_nd,
			sum(::[RUNTIME].[F].A('И'||crt.[ACCOUNT]||'АН', data))					:crt_sm_acc
			
		)in ::[PR_CRED]	
			where crt.[date_begin] < data
			 and (crt.[date_close] > data or crt.[date_close] is null)
			 and crt.[HIGH_LEVEL_CR] = v_f_cred_doc_id
			 group by crt.[HIGH_LEVEL_CR]							
	)loop
		excel.write(v_row_num , 10, crt.crt_sm_acc);
		v_f_v_summa := v_f_v_summa - v_f_cred_cr_sm_acc + crt.crt_sm_acc;
	end loop;
	return v_f_v_summa; 		
end;

------------------------проверка связи у связанных кредитов (функция 2, рекурсивная)------------------------------------------------------------------------------------------------------00-----
function v_check_link_cred_2 (v_fnc_doc_id number , v_fnc_cl_id number, v_date date , v_row_num number, v_group_num number  , p_sum in out number) return number is
	v_retVal_main	number;
	row 	number;
	str 	string;
	rod		number;
	lk      string;
	v_row   number;
	v_main_row number;
	v_summa number;
	v_main_row2 number;
	v_summa2 number;
	ft_unit	::[RECONT].[UNIT]%type;
	v_doc_id number;
	v_dat date;
	data date;
	filial string;
	v_sum number;
	V_cr_a_b number;
	arr_cr_cl varchar2;
	test_num number;
begin
	v_summa2 := 0;
	data := to_date(v_date);
	row:=v_row_num;
	v_row := v_group_num;
	for (
	select distinct cr(
				cr%id																	:doc_id
				,cr.client%id															:cl_id
				,cr.[client].[name]														:cr_cl
				,cr.[num_dog]															:cr_nd
				,cr.[FT_CREDIT].[CUR_SHORT]												:cr_val
				,::[pr_cred].[L_3].Getinfo(cr, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cr.[date_begin]														:cr_db
				,cr.[DATE_ENDING]														:cr_de
				,cr.[SUMMA_DOG]															:cr_sm
				,::[RUNTIME].[F].A('И'||cr.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cr.[date_close]														:cr_dc
				,cr.[FILIAL].[BANK].[BIC]												:cr_bic	
				,cr%class	                                                            :cr_class
			
		)in ::[PR_CRED]
		where cr%id = v_fnc_doc_id
		 	and cr.client%id = v_fnc_cl_id
	)loop
    ---------------------------------------------------для Юр лиц---------------
    if cr.cr_class IN ('KRED_CORP','OVERDRAFTS') then
	     for (select distinct cr(
			cl_c.[all_boss]			:a_b
		)in ::[PR_CRED]
				,(::[CL_CORP] ALL cl_c)
		where cl_c%id = cr.client
			and cr%id = v_fnc_doc_id
		 	and cr.client%id = v_fnc_cl_id
	     )loop
	     V_cr_a_b:= cr.a_b;
	     end loop;	
	end if;
-----------------------------Родственная связь---------------------------------
	for(
		select cl(
			cl%id			: cl_id
		)in ::[client],
			(::[cl_priv] all :cl_p),
			(::[persons_pos] all :pp),
			(::[client]	all :clt),
			(::[relatives] all :rl)
		where pp%collection = V_cr_a_b--cr.a_b
			and pp.[fase] = clt%id
			and cl_p%id = clt%id
			and cl_p.[relatives_client] = rl%collection
			and rl.[cl_priv] = cl%id
			and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
			and ( pp.[WORK_END] > data or  pp.[WORK_END] is null)
	)loop
		for (
			select distinct cred(
				cred%id																	:doc_id
				,cred.client%id															:cl_id
				,cred.[client].[name]													:cr_cl
				,cred.[num_dog]															:cr_nd
				,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
				,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cred.[date_begin]														:cr_db
				,cred.[DATE_ENDING]														:cr_de
				,cred.[SUMMA_DOG]														:cr_sm
				,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cred.[date_close]														:cr_dc
				,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			)
			in ::[PR_CRED]
			where cred.client = cl.cl_id
				and cred.[com_status] in (2047865, 2047879)
				and cred.[date_begin] < data
				and (cred.[date_close] > data or cred.[date_close] is null)
				and cr.cl_id != cred.client%id
				and cred.[HIGH_LEVEL_CR] is null
			order by cred.[client].[name]
		)loop
			if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then		
				excel.write(row, 1, cred.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cred.cr_cl);
				excel.write(row, 4, cred.cr_nd);
				excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cred.cr_val);
				excel.write(row, 8, cred.cr_prc/100);
				excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
				excel.write(row, 10, cred.cr_sm_acc);
				excel.write(row, 11, 'Родственная связь с ');
				excel.write(row, 12, cr.cr_cl);
				v_summa2 := v_summa2 + cred.cr_sm_acc;
				v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
				row:=row+1;
				row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa2 := v_summa2 + v_sum;
				end if;
				lk:='';
			end if;
		end loop;
	end loop;
-------------------------Финансовая зависимость----------------------------
	for (
		select fd(
			l_cl%id :cl_id
		)in ::[CLIENT],	(::[LINKS_CL] all :l_cl) all
		where fd.[links_other] = l_cl%collection
			and fd%id = cr.cl_id
	)loop
	    for (
			select ptr(
				cl%id cl_id
			)in ::[links_cl],(::[client] all cl) all
			where ptr.[partner] = cl%id
				and fd.cl_id = ptr%id
				and (ptr.[DATE_BEG] < data or ptr.[DATE_BEG] is null) --время действия связи
				and (ptr.[DATE_END] > data or ptr.[DATE_END] is null)
		)loop
		    for (
				select distinct cred(
					cred%id																	:doc_id
					,cred.client%id															:cl_id
					,cred.[client].[name]													:cr_cl
					,cred.[num_dog]															:cr_nd
					,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
					,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
					,cred.[date_begin]														:cr_db
					,cred.[DATE_ENDING]														:cr_de
					,cred.[SUMMA_DOG]														:cr_sm
					,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
					,cred.[date_close]														:cr_dc
					,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
				)
				in ::[PR_CRED] all					
				where cred.client = ptr.cl_id
					 and cred.[com_status] in (2047865, 2047879)
					 and cred.[date_begin] < data
					 and (cred.[date_close] > data or cred.[date_close] is null)
					 and cred.[HIGH_LEVEL_CR] is null
			)loop
				if  v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
				    excel.write(row, 1, cred.doc_id);
					excel.write(row, 2, v_row || ' группа');
					excel.write(row, 3, cred.cr_cl);
					excel.write(row, 4, cred.cr_nd);
					excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
					excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
					excel.write(row, 7, cred.cr_val);
					excel.write(row, 8, cred.cr_prc/100);
					excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
					excel.write(row, 10, cred.cr_sm_acc);
					excel.write(row, 11, 'Финансовая зависимость с');
					excel.write(row, 12, cr.cr_cl);
					v_summa2 := v_summa2 + cred.cr_sm_acc;
					v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
					row:=row+1;
					row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
					if v_sum != 0 then
						v_summa2 := v_summa2 + v_sum;
					end if;
					lk:='';
				end if;
			end loop;
		end loop;
	end loop;
	
---------------------------------------------------------------------------
	for(
		select cl(
			cl%id			: cl_id
		)in ::[client],
			(::[cl_priv] all :cl_p),
			(::[persons_pos] all :pp),
			(::[client]	all :clt),
			(::[relatives] all :rl)
		where
			pp%collection = V_cr_a_b--cr.a_b
			and pp.[fase] = clt%id
			and cl_p%id = clt%id
			and cl_p.[relatives_client] = rl%collection
			and rl.[cl_priv] = cl%id
			and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
			and (pp.[WORK_END] > data or  pp.[WORK_END] is null)
	)loop
		for (
			select distinct cred(
				cred%id																	:doc_id
				,cred.client%id															:cl_id
				,cred.[client].[name]													:cr_cl
				,cred.[num_dog]															:cr_nd
				,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
				,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cred.[date_begin]														:cr_db
				,cred.[DATE_ENDING]														:cr_de
				,cred.[SUMMA_DOG]														:cr_sm
				,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cred.[date_close]														:cr_dc
				,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			)in ::[PR_CRED]
			   ,(::[PERSONS_POS] ALL PP)
			   ,(::[CL_CORP] ALL cl_c)
			where cl_c%id = cred.client
				and cl_c.[all_boss] = pp%collection
				and pp.[fase] = cl.cl_id
				and cred.[com_status] in (2047865, 2047879)
				and cred.[date_begin] < data
				and (cred.[date_close] > data or cred.[date_close] is null)
				and cred.[HIGH_LEVEL_CR] is null
				and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
				and ( PP.[WORK_END] > data or  PP.[WORK_END] is null)
		)loop
			if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
				excel.write(row, 1, cred.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cred.cr_cl);
				excel.write(row, 4, cred.cr_nd);
				excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cred.cr_val);
				excel.write(row, 8, cred.cr_prc/100);
				excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
				excel.write(row, 10, cred.cr_sm_acc);
				excel.write(row, 11, 'Финансовая зависимость с');
				excel.write(row, 12, cr.cr_cl);
				v_summa2 := v_summa2 + cred.cr_sm_acc;
				v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
				row:=row+1;
				row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa2 := v_summa2 + v_sum;
				end if;
				lk:='';
			end if;
		end loop;
	end loop;	
		
		
-----------------------------Физ лица (Поручитель)-----------------------------
	for (
		select distinct cred(
			cred%id																	:doc_id
			,cred.client%id															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[guarantees] all :g),
			(::[zalog_body] all :zb) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[name] = cr.cr_cl
			and cred.[client].[name] not like cr.cr_cl
			and zl.[vid_guarantee] = g%id
			and g.[name] like 'Поруч%'
			and pr.[com_status] in (2047865, 2047879)
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then		
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11, 'Заемщик');
			for (
				select distinct cred2(
					cred2.[client].[name]	:cr_cl
				)in ::[PR_CRED],
					(::[client] all :cl),
					(::[part_to_loan] all :ptl),
					(::[zalog] all :zl),
					(::[product] all :pr),
					(::[zalog_values] all :zv),
					(::[guarantees] all :g),
					(::[zalog_body] all :zb) all
				where cred2.[client] = cl%id
					and pr%id = cred2%id
					and zl.[part_to_loan] = ptl%collection
					and ptl.[product] = pr%id
					and zl.[zalog_body] = zv%collection
					and zv.[zalog_body] = zb%id
					and zb.[name] = cred.cr_cl
					and cred2.[client].[name] not like cred.cr_cl
					and zl.[vid_guarantee] = g%id
					and g.[name] like 'Поруч%'
					and pr.[com_status] in (2047865, 2047879)
					and cred2.[date_begin] < data
					and (cred2.[date_close] > data or cred2.[date_close] is null)
					and cred2.[HIGH_LEVEL_CR] is null
					and cred2.[client].[name] is not null
				group by cred2.[client].[name]
			)loop
			if arr_cr_cl is null then
				arr_cr_cl := cred2.cr_cl;
			else
				arr_cr_cl := arr_cr_cl || ' , ' || cred2.cr_cl;
			end if;
			test_num :=1;
			end loop;
			if test_num =1 then
				excel.write(row, 11, 'Поручитель для');
				excel.write(row, 12, arr_cr_cl);
				arr_cr_cl := null;
			end if;
			test_num :=0;
			v_summa2 := v_summa2 + cred.cr_sm_acc;
			v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
			row:=row+1;
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa2 := v_summa2 + v_sum;
			end if;
			lk:='';
		end if;
	end loop;

--------------------------Организация (поручитель)-------------------------
	for(
		select distinct cred(
			cred%id																	:doc_id
			,cred.client%id															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic
			,cl_c.[all_boss]														:ab
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[guarantees] all :g),
			(::[zalog_body] all :zb),
			(::[cl_corp] all    :cl_c) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[name] = cr.cr_cl
			and cred.[client].[name] not like cr.cr_cl
			and zl.[vid_guarantee] = g%id
			and g.[name] like 'Поруч%'
			and pr.[com_status] in (2047865, 2047879)
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cl%id = cl_c%id
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then		
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11,'Заемщик');
			v_summa2 := v_summa2 + cred.cr_sm_acc;
			v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
			row:=row+1;
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa2 := v_summa2 + v_sum;
			end if;
			lk:='';
		end if;
	end loop;
-----------------------------Физ лиц. (Залогодатель)----------------
	for(
		select distinct cred(
			cred%id																	:doc_id
			,cred.[client]															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[zalog_body] all :zb),
			(::[zalog_body_add] all :zba),
			(::[zbadd_house] all zbh),
			(::[client] all :cl2),
			(::[rightsetting_doc] all rd) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[adds] = zba%id
			and zba%id = zbh%id
			and zbh.[doc_descriptions] = rd%collection
			and rd.[client] = cl2%id
			and cl2%id = cr.cl_id
			and pr.[com_status] in (2047865, 2047879)
			and cred.[client].[name] not like cr.cr_cl
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if  v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then	
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11,'Заемщик');
			v_summa2 := v_summa2 + cred.cr_sm_acc;
			v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
			row:=row+1;
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa2 := v_summa2 + v_sum;
			end if;
			lk:='';
		end if;
	end loop;
 -----------------------------Организация (Залогодатель)----------------
	for(
		select distinct cred(
			cred%id																	:doc_id
			,cred.[client]															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			,cl_c.[all_boss]														:cl_c
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[zalog_body] all :zb),
			(::[zalog_body_add] all :zba),
			(::[zbadd_house] all zbh),
			(::[client] all :cl2),
			(::[cl_corp] all :cl_c),
			(::[rightsetting_doc] all rd) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[adds] = zba%id
			and zba%id = zbh%id
			and zbh.[doc_descriptions] = rd%collection
			and rd.[client] = cl2%id
			and cl2%id = cr.cl_id
			and pr.[com_status] in (2047865, 2047879)
			and cred.[client].[name] not like cr.cr_cl
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cl%id = cl_c%id
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then		
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11,'Заемщик');
			v_summa2 := v_summa2 + cred.cr_sm_acc;
			v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
			row:=row+1;
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa2 := v_summa2 + v_sum;
			end if;
			lk:='';
		end if;
	end loop;
	-----------------------------------проверка кредита на явление Долж лицом для другого кредита--------------------
	for (
		select distinct cred(
			cred%id																	:doc_id
			,cred.[client]															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic
			,corp.[all_boss]														:ab
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[cl_corp] all :corp),
			(::[client] all :cl2),
			(::[persons_pos] all :pp),
			(::[product] all :pr) all
		where cl%id = corp%id
			and corp%class = 'CL_ORG'
			and corp.[all_boss] = pp%collection
			and pp.[fase] = cl2%id
			and cred%id = pr%id
			and cred.[client] = cl%id
			and cl2%id = cr.cl_id
			and pr.[com_status] in (2047865, 2047879)
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cred.[HIGH_LEVEL_CR] is null
			and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
			and ( pp.[WORK_END] > data or  pp.[WORK_END] is null)
	)loop
		if  v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11, 'Заемщик ');
			v_summa2 := v_summa2 + cred.cr_sm_acc;
			v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
			row:=row+1;
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa2 := v_summa2 + v_sum;
			end if;
		end if;
	end loop;
	-------------------------------Долж лица для клиента-----------------------------------
		for (
			select distinct cred(
				cred%id																	:doc_id
				,cred.[client]															:cl_id
				,cred.[client].[name]													:cr_cl
				,pr.[num_dog]															:cr_nd
				,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
				,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cred.[date_begin]														:cr_db
				,cred.[DATE_ENDING]														:cr_de
				,cred.[SUMMA_DOG]														:cr_sm
				,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cred.[date_close]														:cr_dc
				,cred.[FILIAL].[BANK].[BIC]												:cr_bic
				,corp.[all_boss]														:ab
			)
			in ::[PR_CRED],
				(::[client] all :cl),
				(::[cl_corp] all :corp),
				(::[client] all :cl2),
				(::[persons_pos] all :pp),
				(::[product] all :pr) all
			where
				 cl%id = corp%id
				 and corp%class = 'CL_ORG'
				 and corp.[all_boss] = pp%collection
				 and pp.[fase] = cl2%id
				 and cred%id = pr%id
				 and cr.cl_id = cl%id
				 and cl2%id =cred.[client]
				 and pr.[com_status] in (2047865, 2047879)
				 and cred.[date_begin] < data
				 and (cred.[date_close] > data or cred.[date_close] is null)
				 and cred.[HIGH_LEVEL_CR] is null
				 and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
				 and ( pp.[WORK_END] > data or  pp.[WORK_END] is null)
			)loop
				if  v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
					excel.write(row, 1, cred.doc_id);
					excel.write(row, 2, v_row || ' группа');
					excel.write(row, 3, cred.cr_cl);
					excel.write(row, 4, cred.cr_nd);
					excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
					excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
					excel.write(row, 7, cred.cr_val);
					excel.write(row, 8, cred.cr_prc/100);
					excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
					excel.write(row, 10, cred.cr_sm_acc);
					excel.write(row, 11, 'Должностное лицо в ');
					excel.write(row, 12, cr.cr_cl);
					v_summa2 := v_summa2 + cred.cr_sm_acc;
					v_summa2 := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa2 ,row);
					row:=row+1;
					row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
					if v_sum != 0 then
						v_summa2 := v_summa2 + v_sum;
					end if;
				 end if;
			end loop;
				-------------------------------проверка кредита в банковских гарантиях ---------------------------
			for (
				select distinct rib_b(
					rib_b%id																	:doc_id
					,rib_b.[NUM_DOG]															:cr_nd
					,rib_b.[PRINCIPAL].[NAME]													:cr_cl
					,rib_b.[GUAR_SUM]															:cr_sm
					,rib_b.[FINTOOL].[CUR_SHORT]												:cr_val
					,rib_b.[REQ_DATE]															:cr_db
					,rib_b.[CLS_DATE]															:cr_de
					,rib_b.[PRC_RATE]                                                           :cr_prc
					,rib_b.[DATE_ENDING]                                                        :t_e
					,rib_b.[DATE_CLOSE]                                                        :t_c
				)in ::[RIB_BANK_GUAR]
				  	where rib_b.[com_status] in (2047865, 2047879)
					and rib_b.[REQ_DATE] <= data
					and (rib_b.[DATE_CLOSE] >= data or rib_b.[DATE_CLOSE] is null)
					and rib_b.[PRINCIPAL].[NAME] = cr.cr_cl
			)loop
		            if v_check_repeat(rib_b.doc_id, rib_b.cr_cl) = 1 then		
					    excel.write(row, 1, rib_b.doc_id);
						excel.write(row, 2, v_row || ' группа');
						excel.write(row, 3, rib_b.cr_cl);
						excel.write(row, 4, rib_b.cr_nd);
						excel.write(row, 5, to_char(rib_b.cr_db, 'mm/dd/yyyy'));
						excel.write(row, 6, to_char(rib_b.cr_de, 'mm/dd/yyyy'));
						excel.write(row, 7, rib_b.cr_val);
						excel.write(row, 8, rib_b.cr_prc/100);
						excel.write(row, 9, rib_b.cr_sm);
						excel.write(row, 10, rib_b.cr_sm);
						excel.write(row, 11, 'Гарантия');
						v_summa2 := v_summa2 + rib_b.cr_sm;	
						row:=row+1;
					end if;
			
			end loop;	
			
		p_sum := v_summa2;
		v_summa2 := 0;
		v_row :=v_row+1;
	end loop;
	v_retVal_main := row;
	return v_retVal_main ;
end;


------------------------проверка связи у связанных кредитов (функция 1 , основная функция)
function v_check_link_cred (v_fnc_doc_id number , v_fnc_cl_id number, v_date date , v_row_num number, v_group_num number) return number is
	v_retVal_main	number;
	row 	number;
	str 	string;
	rod		number;
	lk      string;
	v_row   number ;
	v_main_row number;
	v_summa number;
	v_main_row2 number;
	v_summa2 number;
	ft_unit	::[RECONT].[UNIT]%type;
	v_doc_id number;
	vIdx	number ;
	vIdx_main number;
	v_dat date;
	data date;
	filial string;
	v_variable number;
	v_sum number;
	arr_cr_cl varchar2;
	V_cr_a_b number;
	test_num number;
begin
	data := to_date(v_date);
	vIdx := v_Tab_doc_id.count + 1;
	row:=v_row_num;
	v_row := v_group_num;
	
for (
		select distinct cr(
			cr%id																	:doc_id
			,cr.client%id															:cl_id
			,cr.[client].[name]														:cr_cl
			,cr.[num_dog]															:cr_nd
			,cr.[FT_CREDIT].[CUR_SHORT]												:cr_val
			,::[pr_cred].[L_3].Getinfo(cr, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cr.[date_begin]														:cr_db
			,cr.[DATE_ENDING]														:cr_de
			,cr.[SUMMA_DOG]															:cr_sm
			,::[RUNTIME].[F].A('И'||cr.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cr.[date_close]														:cr_dc
			,cr.[FILIAL].[BANK].[BIC]												:cr_bic	
			,cr%class	                                                            :cr_class
			
		)in ::[PR_CRED]
		where cr%id = v_fnc_doc_id
		 	and cr.client%id = v_fnc_cl_id
		 	and cr.[HIGH_LEVEL_CR] is null
	)loop
    v_main_row:=0;
	v_variable :=0;
	-----------------------------------для Юр лиц-----------------
    if cr.cr_class IN ('KRED_CORP','OVERDRAFTS') then
	     for (select distinct cr(
			cl_c.[all_boss]		:a_b
		)in ::[PR_CRED]
				,(::[CL_CORP] ALL cl_c)
		where cl_c%id = cr.client
			and cr%id = v_fnc_doc_id
		 	and cr.client%id = v_fnc_cl_id
	     )loop
	     V_cr_a_b:= cr.a_b;
	     end loop;	
	end if;
	-------------------------Финансовая зависимость----------------------------
		for (
			select fd(
				l_cl%id :cl_id
			)in ::[CLIENT],
				(::[LINKS_CL] all :l_cl) all
			where fd.[links_other] = l_cl%collection
				and fd%id = cr.cl_id
		)loop
	   		for(
				select ptr(
					cl%id cl_id
				)in ::[links_cl],(::[client] all cl) all
				where ptr.[partner] = cl%id
					and fd.cl_id = ptr%id
					and (ptr.[DATE_BEG] < data or ptr.[DATE_BEG] is null)
					and (ptr.[DATE_END] > data or ptr.[DATE_END] is null)
			)loop
		        for (
					select distinct cred(
						cred%id																	:doc_id
						,cred.client%id															:cl_id
						,cred.[client].[name]													:cr_cl
						,cred.[num_dog]															:cr_nd
						,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
						,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
						,cred.[date_begin]														:cr_db
						,cred.[DATE_ENDING]														:cr_de
						,cred.[SUMMA_DOG]														:cr_sm
						,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
						,cred.[date_close]														:cr_dc
						,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
					)in ::[PR_CRED] all					
					where cred.client = ptr.cl_id
						and cred.[com_status] in (2047865, 2047879)
						and cred.[date_begin] < data
						and (cred.[date_close] > data or cred.[date_close] is null)
						and cred.[HIGH_LEVEL_CR] is null
					order by cred.[client].[name]
				)loop
					if v_check_repeat_23(cred.doc_id,cred.cr_cl) =1  then
			   				
							if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
							    excel.write(row, 1, cr.doc_id);
								excel.write(row, 2, v_row || ' группа');
								excel.write(row, 3, cr.cr_cl);
								excel.write(row, 4, cr.cr_nd);
								excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
								excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
								excel.write(row, 7, cr.cr_val);
								excel.write(row, 8, cr.cr_prc/100);
								excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
								excel.write(row, 10, cr.cr_sm_acc);
								excel.write(row, 11, 'Заемщик');
								v_summa := cr.cr_sm_acc;
								v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
								row:=row+1;
							end if;
							row := v_check_link_cred_2(cr.doc_id , cr.cl_id, data, row, v_row, v_sum );
							if v_sum != 0 then
								v_summa := v_summa + v_sum;
							end if;
							v_main_row :=1;
							v_variable := 1;
					end if;		
				end loop;
			end loop;
		end loop;
		
---------------------------------------------------------------------------
		for (
			select cl(
				cl%id			: cl_id
			)in ::[client],
				(::[cl_priv] all :cl_p),
				(::[persons_pos] all :pp),
				(::[client]	all :clt),
				(::[relatives] all :rl)
			where pp%collection = V_cr_a_b
				and pp.[fase] = clt%id
				and cl_p%id = clt%id
				and cl_p.[relatives_client] = rl%collection
				and rl.[cl_priv] = cl%id
				and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
				and ( pp.[WORK_END] > data or  pp.[WORK_END] is null)
		)loop
			for (
				select distinct cred(
					cred%id																	:doc_id
					,cred.client%id															:cl_id
					,cred.[client].[name]													:cr_cl
					,cred.[num_dog]															:cr_nd
					,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
					,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
					,cred.[date_begin]														:cr_db
					,cred.[DATE_ENDING]														:cr_de
					,cred.[SUMMA_DOG]														:cr_sm
					,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
					,cred.[date_close]														:cr_dc
					,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
				)
				in ::[PR_CRED]
				   ,(::[PERSONS_POS] ALL PP)
				   ,(::[CL_CORP] ALL cl_c)
				where cl_c%id = cred.client
			 		and cl_c.[all_boss] = pp%collection
					and pp.[fase] = cl.cl_id
				    and cred.[com_status] in (2047865, 2047879)
					and cred.[date_begin] < data
					and (cred.[date_close] > data or cred.[date_close] is null)
					and cred.[HIGH_LEVEL_CR] is null
					and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
					and ( PP.[WORK_END] > data or  PP.[WORK_END] is null)
			)loop
				if v_variable = 1 then
					if v_check_repeat_23(cred.doc_id,cred.cr_cl) =1  then
			   			if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
						    excel.write(row, 1, cr.doc_id);
							excel.write(row, 2, v_row || ' группа');
							excel.write(row, 3, cr.cr_cl);
							excel.write(row, 4, cr.cr_nd);
							excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
							excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
							excel.write(row, 7, cr.cr_val);
							excel.write(row, 8, cr.cr_prc/100);
							excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
							excel.write(row, 10, cr.cr_sm_acc);
							excel.write(row, 11, 'Заемщик');
							v_summa := cr.cr_sm_acc;
							v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
							row:=row+1;
						end if;
						row := v_check_link_cred_2(cr.doc_id , cr.cl_id, data, row, v_row, v_sum );
						if v_sum != 0 then
							v_summa := v_summa + v_sum;
						end if;
						v_main_row :=1;
						v_variable := 1;
					end if;
				end if;	
			end loop;
		end loop;	
	
	-----------------------------Родственная связь---------------------------------
	for (
		select cl(
			cl%id			: cl_id
		)in ::[client],
			(::[cl_priv] all :cl_p),
			(::[relatives] all :rl)
		where cl_p%id = cr.cl_id
			and cl_p.[relatives_client] = rl%collection
			and rl.[cl_priv] = cl%id
		)loop
	    for (
	    	select distinct cred(
				cred%id																	:doc_id
				,cred.client%id															:cl_id
				,cred.[client].[name]													:cr_cl
				,cred.[num_dog]															:cr_nd
				,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
				,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cred.[date_begin]														:cr_db
				,cred.[DATE_ENDING]														:cr_de
				,cred.[SUMMA_DOG]														:cr_sm
				,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cred.[date_close]														:cr_dc
				,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			)in ::[PR_CRED]
			where cred.client = cl.cl_id
				and cred.[com_status] in (2047865, 2047879)
			 	and cred.[date_begin] < data
			 	and (cred.[date_close] > data or cred.[date_close] is null)
			 	and cred.[HIGH_LEVEL_CR] is null
			order by cred.[client].[name]
		)loop
			if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then	
				if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
					excel.write(row, 1, cr.doc_id);
					excel.write(row, 2, v_row || ' группа');
					excel.write(row, 3, cr.cr_cl);
					excel.write(row, 4, cr.cr_nd);
					excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
					excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
					excel.write(row, 7, cr.cr_val);
					excel.write(row, 8, cr.cr_prc/100);
					excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
					excel.write(row, 10, cr.cr_sm_acc);
					excel.write(row, 11, 'Родственная связь');
					excel.write(row, 12, cred.cr_cl);
					v_summa := cr.cr_sm_acc;
					v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
					row:=row+1;
				end if;
				excel.write(row, 1, cred.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cred.cr_cl);
				excel.write(row, 4, cred.cr_nd);
				excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cred.cr_val);
				excel.write(row, 8, cred.cr_prc/100);
				excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
				excel.write(row, 10, cred.cr_sm_acc);
				excel.write(row, 11, 'Заемщик');
				v_summa := v_summa + cred.cr_sm_acc;	
				v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
				row:=row+1;
				row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa := v_summa + v_sum;
				end if;
				v_main_row :=1;
				v_variable := 1;
			end if;	
		end loop;
	end loop;
	----------проверка поручителя на своего поручителя -----------
	for (
		select distinct cr2(
				cr2%id																	:doc_id
				,cr2.client%id															:cl_id
				,cr2.[client].[name]													:cr_cl
				,cr2.[num_dog]															:cr_nd
				,cr2.[FT_CREDIT].[CUR_SHORT]											:cr_val
				,::[pr_cred].[L_3].Getinfo(cr2, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cr2.[date_begin]														:cr_db
				,cr2.[DATE_ENDING]														:cr_de
				,cr2.[SUMMA_DOG]														:cr_sm
				,::[RUNTIME].[F].A('И'||cr2.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cr2.[date_close]														:cr_dc
				,cr2.[FILIAL].[BANK].[BIC]												:cr_bic	
			)in ::[PR_CRED]
			where
				 cr2.[com_status] in (2047865, 2047879)
				 and cr2.[date_begin] < data
				 and (cr2.[date_close] > data or cr2.[date_close] is null)
				 AND cr2.[KIND_CREDIT].[CODE] != 'TECH_OVER'
				 and cr2.[HIGH_LEVEL_CR] is null
				 and cr2.[client].[name] in  (select distinct cred2(
										          zb2.[name]  						
										        )in ::[PR_CRED],
										          (::[client] all :cl2),
										          (::[part_to_loan] all :ptl2),
										          (::[zalog] all :zl2),
										          (::[product] all :pr2),
										          (::[zalog_values] all :zv2),
										          (::[guarantees] all :g2),
										          (::[zalog_body] all :zb2) all
										        where cred2.[client] = cl2%id
										          and pr2%id = cred2%id
										          and zl2.[part_to_loan] = ptl2%collection
										          and ptl2.[product] = pr2%id
										          and zl2.[zalog_body] = zv2%collection
										          and zv2.[zalog_body] = zb2%id
										          and cred2.[client].[name] = cr.cr_cl
										          and zl2.[vid_guarantee] = g2%id
										          and g2.[name] like 'Поруч%'
										          and pr2.[com_status] in (2047865, 2047879)
										          and cred2.[date_begin] < data
										          and (cred2.[date_close] > data or cred2.[date_close] is null)
										          and cred2.[HIGH_LEVEL_CR] is null )
	)loop
		for (
			select distinct cred(
				cred%id																	:doc_id
				,cred.client%id															:cl_id
				,cred.[client].[name]													:cr_cl
				,pr.[num_dog]															:cr_nd
				,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
				,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cred.[date_begin]														:cr_db
				,cred.[DATE_ENDING]														:cr_de
				,cred.[SUMMA_DOG]														:cr_sm
				,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cred.[date_close]														:cr_dc
				,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			)in ::[PR_CRED],
				(::[client] all :cl),
				(::[part_to_loan] all :ptl),
				(::[zalog] all :zl),
				(::[product] all :pr),
				(::[zalog_values] all :zv),
				(::[guarantees] all :g),
				(::[zalog_body] all :zb) all
			where cred.[client] = cl%id
				and pr%id = cred%id
				and zl.[part_to_loan] = ptl%collection
				and ptl.[product] = pr%id
				and zl.[zalog_body] = zv%collection
				and zv.[zalog_body] = zb%id
				and zb.[name] = cr2.cr_cl
				and cred.[client].[name] not like cr2.cr_cl
				and zl.[vid_guarantee] = g%id
				and g.[name] like 'Поруч%'
				and pr.[com_status] in (2047865, 2047879)
				and cred.[date_begin] < data
				and (cred.[date_close] > data or cred.[date_close] is null)
				and cred.[HIGH_LEVEL_CR] is null
		)loop
			if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
				 if v_check_repeat(cr2.doc_id,cr2.cr_cl) =1 and v_main_row = 0 then
					excel.write(row, 1, cr2.doc_id);
					excel.write(row, 2, v_row || ' группа');
					excel.write(row, 3, cr2.cr_cl);
					excel.write(row, 4, cr2.cr_nd);
					excel.write(row, 5, to_char(cr2.cr_db, 'mm/dd/yyyy'));
					excel.write(row, 6, to_char(cr2.cr_de, 'mm/dd/yyyy'));
					excel.write(row, 7, cr2.cr_val);
					excel.write(row, 8, cr2.cr_prc/100);
					excel.write(row, 9, cr2.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr2.cr_val), data, ft_unit)));
					excel.write(row, 10, cr2.cr_sm_acc);
					excel.write(row, 11, 'Поручитель для');
					for (
						select distinct cred2(
							cred2.[client].[name]	:cr_cl
						)in ::[PR_CRED],
							(::[client] all :cl),
							(::[part_to_loan] all :ptl),
							(::[zalog] all :zl),
							(::[product] all :pr),
							(::[zalog_values] all :zv),
							(::[guarantees] all :g),
							(::[zalog_body] all :zb) all
						where cred2.[client] = cl%id
							and pr%id = cred2%id
							and zl.[part_to_loan] = ptl%collection
							and ptl.[product] = pr%id
							and zl.[zalog_body] = zv%collection
							and zv.[zalog_body] = zb%id
							and zb.[name] = cr2.cr_cl
							and cred2.[client].[name] not like cr2.cr_cl
							and zl.[vid_guarantee] = g%id
							and g.[name] like 'Поруч%'
							and pr.[com_status] in (2047865, 2047879)
							and cred2.[date_begin] < data
							and (cred2.[date_close] > data or cred2.[date_close] is null)
							and cred2.[HIGH_LEVEL_CR] is null
						group by cred2.[client].[name]
					)loop
					if arr_cr_cl is null then
						arr_cr_cl := cred2.cr_cl;
					else
						arr_cr_cl := arr_cr_cl || ' , ' || cred2.cr_cl;
					end if;
					end loop;
					excel.write(row, 12, arr_cr_cl);
					arr_cr_cl := null;
					v_summa := cr2.cr_sm_acc;
					v_summa := f_check_saldo(cr2.doc_id ,data , cr2.cr_sm_acc ,v_summa ,row);
					row:=row+1;
				end if;
				excel.write(row, 1, cred.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cred.cr_cl);
				excel.write(row, 4, cred.cr_nd);
				excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cred.cr_val);
				excel.write(row, 8, cred.cr_prc/100);
				excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
				excel.write(row, 10, cred.cr_sm_acc);
				excel.write(row, 11, 'Заемщик ');
				for (
					select distinct cred2(
						cred2.[client].[name]	:cr_cl
					)in ::[PR_CRED],
						(::[client] all :cl),
						(::[part_to_loan] all :ptl),
						(::[zalog] all :zl),
						(::[product] all :pr),
						(::[zalog_values] all :zv),
						(::[guarantees] all :g),
						(::[zalog_body] all :zb) all
					where cred2.[client] = cl%id
						and pr%id = cred2%id
						and zl.[part_to_loan] = ptl%collection
						and ptl.[product] = pr%id
						and zl.[zalog_body] = zv%collection
						and zv.[zalog_body] = zb%id
						and zb.[name] = cred.cr_cl
						and cred2.[client].[name] not like cred.cr_cl
						and zl.[vid_guarantee] = g%id
						and g.[name] like 'Поруч%'
						and pr.[com_status] in (2047865, 2047879)
						and cred2.[date_begin] < data
						and (cred2.[date_close] > data or cred2.[date_close] is null)
						and cred2.[HIGH_LEVEL_CR] is null
						and cred2.[client].[name] is not null
					group by cred2.[client].[name]
				)loop
				if arr_cr_cl is null then
					arr_cr_cl := cred2.cr_cl;
				else
					arr_cr_cl := arr_cr_cl || ' , ' || cred2.cr_cl;
				end if;
				test_num :=1;
				end loop;
				if test_num =1 then
					excel.write(row, 11, 'Поручитель для');
					excel.write(row, 12, arr_cr_cl);
					arr_cr_cl := null;
				end if;
				test_num :=0;
				v_summa := v_summa + cred.cr_sm_acc;	
				v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
				row:=row+1;
				lk:='';
				row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa := v_summa + v_sum;
				end if;
				row := v_check_link_cred_2(cr2.doc_id , cr2.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa := v_summa + v_sum;
				end if;
				v_main_row:=1;
				v_variable :=1;
			end if;
		end loop;
	end loop;
	
	-----------------------------Физ лица (поручитель)-----------------------------
	for (
		select distinct cred(
			cred%id																	:doc_id
			,cred.client%id															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[guarantees] all :g),
			(::[zalog_body] all :zb) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[name] = cr.cr_cl
			and cred.[client].[name] not like cr.cr_cl
			and zl.[vid_guarantee] = g%id
			and g.[name] like 'Поруч%'
			and pr.[com_status] in (2047865, 2047879)
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
			if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
				excel.write(row, 1, cr.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cr.cr_cl);
				excel.write(row, 4, cr.cr_nd);
				excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cr.cr_val);
				excel.write(row, 8, cr.cr_prc/100);
				excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
				excel.write(row, 10, cr.cr_sm_acc);
				excel.write(row, 11, 'Поручитель для');
				for (
					select distinct cred2(
						cred2.[client].[name]							:cr_cl
					)in ::[PR_CRED],
						(::[client] all :cl),
						(::[part_to_loan] all :ptl),
						(::[zalog] all :zl),
						(::[product] all :pr),
						(::[zalog_values] all :zv),
						(::[guarantees] all :g),
						(::[zalog_body] all :zb) all
					where cred2.[client] = cl%id
						and pr%id = cred2%id
						and zl.[part_to_loan] = ptl%collection
						and ptl.[product] = pr%id
						and zl.[zalog_body] = zv%collection
						and zv.[zalog_body] = zb%id
						and zb.[name] = cr.cr_cl
						and cred2.[client].[name] not like cr.cr_cl
						and zl.[vid_guarantee] = g%id
						and g.[name] like 'Поруч%'
						and pr.[com_status] in (2047865, 2047879)
						and cred2.[date_begin] < data
						and (cred2.[date_close] > data or cred2.[date_close] is null)
						and cred2.[HIGH_LEVEL_CR] is null
						group by cred2.[client].[name]
				)loop
				if arr_cr_cl is null then
					arr_cr_cl := cred2.cr_cl;
				else
					arr_cr_cl := arr_cr_cl || ' , ' || cred2.cr_cl;
				end if;
				end loop;
				excel.write(row, 12, arr_cr_cl);
				arr_cr_cl := null;
				v_summa := cr.cr_sm_acc;
				v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
				row:=row+1;
			end if;
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11, 'Заемщик ');
			v_summa := v_summa + cred.cr_sm_acc;	
			v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
			row:=row+1;
			lk:='';
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa := v_summa + v_sum;
			end if;
			v_main_row:=1;
			v_variable :=1;
		end if;
	end loop;
	
	
	--------------------------Физ лица/Организация (Поручитель)-------------------------
	for (
		select distinct cred(
			cred%id																	:doc_id
			,cred.client%id															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic
			,cl_c.[all_boss]														:ab
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[guarantees] all :g),
			(::[zalog_body] all :zb),
			(::[cl_corp] all    :cl_c) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[name] = cr.cr_cl
			and cred.[client].[name] not like cr.cr_cl
			and zl.[vid_guarantee] = g%id
			and g.[name] like 'Поруч%'
			and pr.[com_status] in (2047865, 2047879)
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cl%id = cl_c%id
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if  v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
	        if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
				excel.write(row, 1, cr.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cr.cr_cl);
				excel.write(row, 4, cr.cr_nd);
				excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cr.cr_val);
				excel.write(row, 8, cr.cr_prc/100);
				excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
				excel.write(row, 10, cr.cr_sm_acc);
				excel.write(row, 11, 'Поручитель для ');
				for (
					select distinct cred2(
						cred2.[client].[name]													:cr_cl
					)in ::[PR_CRED],
						(::[client] all :cl),
						(::[part_to_loan] all :ptl),
						(::[zalog] all :zl),
						(::[product] all :pr),
						(::[zalog_values] all :zv),
						(::[guarantees] all :g),
						(::[zalog_body] all :zb),
						(::[cl_corp] all    :cl_c) all
					where cred2.[client] = cl%id
						and pr%id = cred2%id
						and zl.[part_to_loan] = ptl%collection
						and ptl.[product] = pr%id
						and zl.[zalog_body] = zv%collection
						and zv.[zalog_body] = zb%id
						and zb.[name] = cr.cr_cl
						and cred2.[client].[name] not like cr.cr_cl
						and zl.[vid_guarantee] = g%id
						and g.[name] like 'Поруч%'
						and pr.[com_status] in (2047865, 2047879)
						and cred2.[date_begin] < data
						and (cred2.[date_close] > data or cred2.[date_close] is null)
						and cl%id = cl_c%id
						and cred2.[HIGH_LEVEL_CR] is null
				)loop
				if arr_cr_cl is null then
					arr_cr_cl := cred2.cr_cl;
				else
					arr_cr_cl := arr_cr_cl || ' , ' || cred2.cr_cl;
				end if;	
				end loop;
				excel.write(row, 12, arr_cr_cl);
				arr_cr_cl := null;
				v_summa := cr.cr_sm_acc;
				v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
				row:=row+1;
			end if;
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11, 'Заемщик ');
			v_summa := v_summa + cred.cr_sm_acc;	
			v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
			row:=row+1;
			lk:='';
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa := v_summa + v_sum;
			end if;
			row := v_check_link_cred_2(cr.doc_id , cr.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa := v_summa + v_sum;
			end if;
			v_main_row:=1;
			v_variable :=1;
		end if;
	end loop;
				
	------------------------------Физ лиц. (Залогодатель)----------------
	for(
		select distinct cred(
			cred%id																	:doc_id
			,cred.[client]															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[zalog_body] all :zb),
			(::[zalog_body_add] all :zba),
			(::[zbadd_house] all zbh),
			(::[client] all :cl2),
			(::[rightsetting_doc] all rd) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[adds] = zba%id
			and zba%id = zbh%id
			and zbh.[doc_descriptions] = rd%collection
			and rd.[client] = cl2%id
			and cl2%id = cr.cl_id
			and pr.[com_status] in (2047865, 2047879)
			and cred.[client].[name] not like cr.cr_cl
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cred.[HIGH_LEVEL_CR] is null
	)loop	
		if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
			if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
				excel.write(row, 1, cr.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cr.cr_cl);
				excel.write(row, 4, cr.cr_nd);
				excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cr.cr_val);
				excel.write(row, 8, cr.cr_prc/100);
				excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
				excel.write(row, 10, cr.cr_sm_acc);
				excel.write(row, 11, 'Залогодатель для ');
				excel.write(row, 12, cred.cr_cl);
				v_summa := cr.cr_sm_acc;
				v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
				row:=row+1;
			end if;
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11, 'Заемщик');
			v_summa := v_summa + cred.cr_sm_acc;	
			v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
			row:=row+1;
			lk:='';
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa := v_summa + v_sum;
			end if;
			v_main_row:=1;
			v_variable :=1;
		end if;
	end loop;
	-----------------------------Организация (Залогодатель)----------------
	for (
		select distinct cred(
			cred%id																	:doc_id
			,cred.[client]															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			,cl_c.[all_boss]														:cl_c
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[part_to_loan] all :ptl),
			(::[zalog] all :zl),
			(::[product] all :pr),
			(::[zalog_values] all :zv),
			(::[zalog_body] all :zb),
			(::[zalog_body_add] all :zba),
			(::[zbadd_house] all zbh),
			(::[client] all :cl2),
			(::[cl_corp] all :cl_c),
			(::[rightsetting_doc] all rd) all
		where cred.[client] = cl%id
			and pr%id = cred%id
			and zl.[part_to_loan] = ptl%collection
			and ptl.[product] = pr%id
			and zl.[zalog_body] = zv%collection
			and zv.[zalog_body] = zb%id
			and zb.[adds] = zba%id
			and zba%id = zbh%id
			and zbh.[doc_descriptions] = rd%collection
			and rd.[client] = cl2%id
			and cl2%id = cr.cl_id
			and pr.[com_status] in (2047865, 2047879)
			and cred.[client].[name] not like cr.cr_cl
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cl%id = cl_c%id
			and cred.[HIGH_LEVEL_CR] is null
	)loop
		if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then		
			if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
				excel.write(row, 1, cr.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cr.cr_cl);
				excel.write(row, 4, cr.cr_nd);
				excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cr.cr_val);
				excel.write(row, 8, cr.cr_prc/100);
				excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
				excel.write(row, 10, cr.cr_sm_acc);
				excel.write(row, 11, 'Залогодатель для  ');
				excel.write(row, 12, cred.cr_cl);
				v_summa := cr.cr_sm_acc;
				v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
				row:=row+1;
			end if;
			excel.write(row, 1, cred.doc_id);
			excel.write(row, 2, v_row || ' группа');
			excel.write(row, 3, cred.cr_cl);
			excel.write(row, 4, cred.cr_nd);
			excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
			excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
			excel.write(row, 7, cred.cr_val);
			excel.write(row, 8, cred.cr_prc/100);
			excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
			excel.write(row, 10, cred.cr_sm_acc);
			excel.write(row, 11, 'Заемщик');
			v_summa := v_summa + cred.cr_sm_acc;	
			v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
			row:=row+1;
			lk:='';
			row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
			if v_sum != 0 then
				v_summa := v_summa + v_sum;
			end if;
			v_main_row:=1;
			v_variable := 1;
		end if;
	end loop;
				
	-----------------------------------Долж лица--------------------
	for (
		select distinct cred(
			cred%id																	:doc_id
			,cred.[client]															:cl_id
			,cred.[client].[name]													:cr_cl
			,pr.[num_dog]															:cr_nd
			,cred.[FT_CREDIT].[CUR_SHORT]											:cr_val
			,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
			,cred.[date_begin]														:cr_db
			,cred.[DATE_ENDING]														:cr_de
			,cred.[SUMMA_DOG]														:cr_sm
			,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
			,cred.[date_close]														:cr_dc
			,cred.[FILIAL].[BANK].[BIC]												:cr_bic
			,corp.[all_boss]														:ab
		)in ::[PR_CRED],
			(::[client] all :cl),
			(::[cl_corp] all :corp),
			(::[client] all :cl2),
			(::[persons_pos] all :pp),
			(::[product] all :pr) all
		where cl%id = corp%id
			and corp%class = 'CL_ORG'
			and corp.[all_boss] = pp%collection
			and pp.[fase] = cl2%id
			and cred%id = pr%id
			and cred.[client] = cl%id
			and cl2%id = cr.cl_id
			and pr.[com_status] in (2047865, 2047879)
			and cred.[date_begin] < data
			and (cred.[date_close] > data or cred.[date_close] is null)
			and cred.[HIGH_LEVEL_CR] is null
			and (pp.[WORK_BEGIN] < data or pp.[WORK_BEGIN] is null)
			and ( pp.[WORK_END] > data or  pp.[WORK_END] is null)
		)loop
			if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then
			    if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
					excel.write(row, 1, cr.doc_id);
					excel.write(row, 2, v_row || ' группа');
					excel.write(row, 3, cr.cr_cl);
					excel.write(row, 4, cr.cr_nd);
					excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
					excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
					excel.write(row, 7, cr.cr_val);
					excel.write(row, 8, cr.cr_prc/100);
					excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
					excel.write(row, 10, cr.cr_sm_acc);
					excel.write(row, 11, 'Должностное лицо в ');
					excel.write(row, 12, cred.cr_cl);
					v_summa := cr.cr_sm_acc;
					v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
					row:=row+1;
				end if;
				excel.write(row, 1, cred.doc_id);
				excel.write(row, 2, v_row || ' группа');
				excel.write(row, 3, cred.cr_cl);
				excel.write(row, 4, cred.cr_nd);
				excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
				excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
				excel.write(row, 7, cred.cr_val);
				excel.write(row, 8, cred.cr_prc/100);
				excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
				excel.write(row, 10, cred.cr_sm_acc);
				excel.write(row, 11, 'Заемщик');
				v_summa := v_summa + cred.cr_sm_acc;	
				v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
				row:=row+1;
				lk:='';
				row := v_check_link_cred_2(cr.doc_id , cr.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa := v_summa + v_sum;
				end if;
				row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
				if v_sum != 0 then
					v_summa := v_summa + v_sum;
				end if;
				v_main_row:=1;
				v_variable := 1;
			end if;
		end loop;	
		
			-------------------------------другие кредиты клиента---------------------------
		for (
			select distinct cred(
				cred%id																	:doc_id
				,cred.client%id															:cl_id
				,cred.[client].[name]														:cr_cl
				,cred.[num_dog]															:cr_nd
				,cred.[FT_CREDIT].[CUR_SHORT]												:cr_val
				,::[pr_cred].[L_3].Getinfo(cred, 'ПРОЦЕНТНАЯ_СТАВКА' ,data)				:cr_prc
				,cred.[date_begin]														:cr_db
				,cred.[DATE_ENDING]														:cr_de
				,cred.[SUMMA_DOG]															:cr_sm
				,::[RUNTIME].[F].A('И'||cred.[ACCOUNT]||'АН', data)						:cr_sm_acc
				,cred.[date_close]														:cr_dc
				,cred.[FILIAL].[BANK].[BIC]												:cr_bic	
			)in ::[PR_CRED]
			where cred.[com_status] in (2047865, 2047879)
				and cred.[date_begin] < data
				and (cred.[date_close] > data or cred.[date_close] is null)
				and (cred%class IN ('KRED_CORP','KRED_PERS','OVERDRAFTS') AND CRED.[KIND_CREDIT].[CODE] != 'TECH_OVER')
				and cred.[client].[name] = cr.cr_cl
				and cred.[HIGH_LEVEL_CR] is null
				and cred%id != cr.doc_id
		)loop
	        if v_variable = 1 then
				if v_check_repeat(cred.doc_id,cred.cr_cl) = 1 then		
				    if v_check_repeat(cr.doc_id,cr.cr_cl) =1 and v_main_row = 0 then
				        excel.write(row, 1, cr.doc_id);
						excel.write(row, 2, v_row || ' группа');
						excel.write(row, 3, cr.cr_cl);
						excel.write(row, 4, cr.cr_nd);
						excel.write(row, 5, to_char(cr.cr_db, 'mm/dd/yyyy'));
						excel.write(row, 6, to_char(cr.cr_de, 'mm/dd/yyyy'));
						excel.write(row, 7, cr.cr_val);
						excel.write(row, 8, cr.cr_prc/100);
						excel.write(row, 9, cr.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cr.cr_val), data, ft_unit)));
						excel.write(row, 10, cr.cr_sm_acc);
						excel.write(row, 11, 'Заемщик');
						v_summa := cr.cr_sm_acc;
						v_summa := f_check_saldo(cr.doc_id ,data , cr.cr_sm_acc ,v_summa ,row);
						row:=row+1;
					end if;
					excel.write(row, 1, cred.doc_id);
					excel.write(row, 2, v_row || ' группа');
					excel.write(row, 3, cred.cr_cl);
					excel.write(row, 4, cred.cr_nd);
					excel.write(row, 5, to_char(cred.cr_db, 'mm/dd/yyyy'));
					excel.write(row, 6, to_char(cred.cr_de, 'mm/dd/yyyy'));
					excel.write(row, 7, cred.cr_val);
					excel.write(row, 8, cred.cr_prc/100);
					excel.write(row, 9, cred.cr_sm * (::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CUR_SHORT] = cred.cr_val), data, ft_unit)));
					excel.write(row, 10, cred.cr_sm_acc);
					excel.write(row, 11, 'Заемщик');
					v_summa := v_summa + cred.cr_sm_acc;	
					v_summa := f_check_saldo(cred.doc_id ,data , cred.cr_sm_acc ,v_summa ,row);
					row:=row+1;
					row := v_check_link_cred_2(cred.doc_id , cred.cl_id, data, row, v_row, v_sum );
					if v_sum != 0 then
						v_summa := v_summa + v_sum;
					end if;
					v_main_row :=1;
				end if;
		 	end if;	
		end loop;	
		
		-------------------------------проверка кредита в банковских гарантиях ---------------------------
		for (
				select distinct rib_b(
					rib_b%id																	:doc_id
					,rib_b.[NUM_DOG]															:cr_nd
					,rib_b.[PRINCIPAL].[NAME]													:cr_cl
					,rib_b.[GUAR_SUM]															:cr_sm
					,rib_b.[FINTOOL].[CUR_SHORT]												:cr_val
					,rib_b.[REQ_DATE]															:cr_db
					,rib_b.[CLS_DATE]															:cr_de
					,rib_b.[PRC_RATE]                                                           :cr_prc
					,rib_b.[DATE_ENDING]                                                        :t_e
					,rib_b.[DATE_CLOSE]                                                         :t_c
				)in ::[RIB_BANK_GUAR]
				  	where rib_b.[com_status] in (2047865, 2047879)
					and rib_b.[REQ_DATE] <= data
					and (rib_b.[DATE_CLOSE] >= data or rib_b.[DATE_CLOSE] is null)
					and rib_b.[PRINCIPAL].[NAME] = cr.cr_cl
			)loop
				if v_variable =1 then
		            if v_check_repeat(rib_b.doc_id, rib_b.cr_cl) = 1 then	
		          	    excel.write(row, 1, rib_b.doc_id);
						excel.write(row, 2, v_row || ' группа');
						excel.write(row, 3, rib_b.cr_cl);
						excel.write(row, 4, rib_b.cr_nd);
						excel.write(row, 5, to_char(rib_b.cr_db, 'mm/dd/yyyy'));
						excel.write(row, 6, to_char(rib_b.cr_de, 'mm/dd/yyyy'));
						excel.write(row, 7, rib_b.cr_val);
						excel.write(row, 8, rib_b.cr_prc/100);
						excel.write(row, 9, rib_b.cr_sm);
						excel.write(row, 10, rib_b.cr_sm);
						excel.write(row, 11, 'Гарантия');
						v_summa := v_summa + rib_b.cr_sm;	
						row:=row+1;
					end if;
				end if;
			end loop;	
		
		if v_main_row =1 then
			excel.write(row, 3, 'Итого по группе '|| v_row ||' :');
			excel.write(row, 10, v_summa );
			v_summa := 0;
			row:=row+2;
			v_row :=v_row+1;
			v_variable := 0;
		end if;
	end loop;
	v_retVal_main := row;
	return v_retVal_main ;
end;


--------------------------Процедура----------------------------------------------
procedure DRAW_REPORT(data date, filial string) is
	row 	number;
	str 	string;
	rod		number;
	lk      string;
	v_row   number :=1;
	v_main_row number;
	v_summa number;
	v_main_row2 number;
	v_summa2 number;
	ft_unit	::[RECONT].[UNIT]%type;
	v_doc_id number;
	vIdx	number ;
	vIdx_main number;
	v_dat_test number;
	ii number;
	v_sum number;
begin
	v_Tab_doc_id:=null;
	vIdx :=1;
	vIdx_main :=1;
	row:=6;
	v_sum :=0;
	
 	EXCEL.WRITE(1, 13, ::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CODE_ISO] = '840'), data, ft_unit));
	EXCEL.WRITE(2, 13, ::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CODE_ISO] = '978'), data, ft_unit));
	EXCEL.WRITE(3, 13, ::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CODE_ISO] = '643'), data, ft_unit));

--------------------------------Кредиты ------------------------------
	for (
	select distinct cr(
				cr%id																	:doc_id
				,cr.client%id															:cl_id
				,cr.[client].[name]														:cr_cl
			)in ::[PR_CRED]
			where cr.[com_status] in (2047865, 2047879)
				 and cr.[date_begin] < data
				 and (cr.[date_close] > data or cr.[date_close] is null)
				 and (cr%class IN ('KRED_CORP','KRED_PERS','OVERDRAFTS') AND CR.[KIND_CREDIT].[CODE] != 'TECH_OVER')
				 --and cr.[client].[name] in ('Мусуралиев Канатбек Керималиевич','Магомедов Мурат Курбанович','Конев Алексей Валерьянович')
			order by cr.[client].[name]
	)loop
		v_dat_test := row;
		row := v_check_link_cred(cr.doc_id , cr.cl_id, data, row, v_row );
   		if v_dat_test != row then
			v_row := v_row +1;
		end if;
	end loop;

	str := 'Отчет по связанным кредитам ОАО "Кереметбанк" на '|| to_char(data,'dd/mm/yyyy');
	excel.write(2, 4, str);	
	v_Tab_doc_id := null;
end;
