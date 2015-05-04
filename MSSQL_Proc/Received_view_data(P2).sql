CREATE procedure [dbo].[received_view_data]  
as  
-- exec received_view_data  
declare  
@tablename varchar(30)  
,@view_name varchar(30)  
,@p_key varchar(30)  
,@mergeqry nvarchar(max)  
,@srccolumncsv varchar(max)  
,@trgcolumncsv varchar(max)  
,@cnt int  
,@ctr int =0  
,@selxml nvarchar(max)=''  
,@column_name varchar(30)  
,@column_type varchar(30)  
,@column_length int,@parmdefinition nvarchar(max)  
,@updmrg varchar(max)=''  
, @recvreqdlghandle uniqueidentifier  
, @sb_msg xml  
, @recvreqmsgname sysname  
set @parmdefinition = N'@tablename varchar(30),@selxml varchar(max),@p_key varchar(30),@trgcolumncsv varchar(max),@srccolumncsv varchar(max),@updmrg varchar(max),@sb_msg xml';  
set nocount on  
begin  
begin try  
 IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(N'[#columnlist]'))  
  drop table #columnlist  
 create table #columnlist  
  (column_name varchar(30),  
  column_type varchar(30),  
  column_length int,  
  col_position int)  
-- creatig a loop to get the message one by one from queue  
 while (1=1)  
 begin  
  begin transaction;  
   waitfor( receive top(1)  
     @recvreqdlghandle = conversation_handle,  
     @sb_msg = message_body,  
     @recvreqmsgname = message_type_name  
     from oubdestq  
    ), timeout 1000;  
   if (@@rowcount = 0)  
    begin  
     rollback transaction;  
     break;  
    end  
   --select @tablename  
   --select @sb_msg  
   select @view_name = c.value('(TABLE_NAME)[1]', 'varchar(30)') from @sb_msg.nodes('/Data/row') t(c)  
   select @tablename = (select replace(@view_name,'_old',''))  
   --Select @tablename  
   --select @p_key  
   set @p_key=(select column_name from information_schema.key_column_usage  
   where objectproperty(object_id(constraint_name), 'isprimarykey') = 1 and table_name = @tablename)  
   --select @p_key  
   set @srccolumncsv =(select  stuff((select ', ' + 'srctab.'+column_name from information_schema.columns  
    where table_name = @tablename order by ordinal_position for xml path(''), type) .value('.','varchar(max)'),1,2,' ') )  
   --select @srccolumncsv  
   set @trgcolumncsv = (select replace(@srccolumncsv,'srctab.',''))  
   --select * from #columnlist  
   truncate table #columnlist  
   insert #columnlist select column_name,data_type,character_maximum_length,ordinal_position from information_schema.columns where table_name = @view_name order by ordinal_position  
   set @cnt=(select count(column_name) from #columnlist)  
   set @ctr = 0  
   set @selxml = ''  
   set @updmrg = ''  
   while (@cnt <> @ctr)  
    begin  
     set @ctr = @ctr+1  
     select @column_name=column_name, @column_type=column_type,@column_length=column_length from #columnlist  
     where col_position = @ctr  
     if len(@selxml) > 0  
     begin  
      set @selxml = @selxml + ', '  
     end  
     if len(@updmrg) > 0 and substring(@updmrg,len(@updmrg) ,1) <>','  
     begin       
      set @updmrg =  @updmrg  + ', '  
     end  
     --set @column_name = replace(replace(@column_name ,'$','_x0024_'),'#','_x0023_')  
     --if @column_type='datetime'  
     --begin  
     -- set @column_type = 'varchar(24)'  
     --end  
     --else if @column_type='int'  
     --begin  
     -- set @column_type = 'varchar(18)'  
     --end  
     --else if @column_type='numeric'  
     --begin  
     -- set @column_type = 'varchar(18)'  
     --end  
     select @selxml = @selxml + ' d.value(''('+@column_name +')[1]'', ' + case when @column_length is not null then (''''+@column_type+'('+case when @column_length =-1 then 'MAX' else cast(@column_length as varchar) end +')''') else ''''+@column_type+''''
 end + ') as '+  @column_name  
  
     if @column_name <> @p_key  
     begin  
      select @updmrg = @updmrg + 'tartab.'+@column_name+' = srctab.'+ @column_name  
     end  
       
    end  
      
   set @selxml = 'select '+@selxml+' from @sb_msg.nodes(''/Data/row'') t(d)'  
   --select @selxml  
   --Select @tablename  
   --select @p_key  
   --select @trgcolumncsv  
   --select @srccolumncsv  
   --select @updmrg  
   set @mergeqry = N'merge '  
   +@tablename+' as tartab  
   using ('+@selxml+') as srctab  
   on (tartab.'+@p_key+' = srctab.'+@p_key+')  
   when not matched by target  
   then insert('+@trgcolumncsv+') values('+@srccolumncsv+')  
   when matched  
   then update set '+ @updmrg +';'  
   --select @mergeqry  
   execute sp_executesql @mergeqry,@parmdefinition,@tablename=@tablename,@selxml=@selxml,@p_key=@p_key,@trgcolumncsv=@trgcolumncsv,@srccolumncsv=@srccolumncsv,@updmrg=@updmrg,@sb_msg=@sb_msg  
  end conversation @recvreqdlghandle --WITH CLEANUP  
  commit transaction;  
 end  
 end try   
 begin catch  
  declare @error int, @message nvarchar(4000);  
  select @error = ERROR_NUMBER(), @message = ERROR_MESSAGE();  
  end conversation @recvreqdlghandle WITH CLEANUP --with error = @error description = @message;  
  insert into ssb_error_log(model_name,ssb_msg,error_no,error_msg)values(@tablename,@mergeqry,@error,@message)  
 end catch  
end  
--  select cast(message_body as xml),*  from oubdestq --163711  
-- SELECT * FROM v_model_members WHERE MBR_IDN >= 163711  
-- received_view_data  
-- SELECT * FROM oubdestq  
-- dbcc opentran  
  
  