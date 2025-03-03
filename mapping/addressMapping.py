from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Select, Delete, Update


@Component
class AddressMapping:
    # @Select("select * from #{table} where op_flag!=9 order by id limit #{page_size} offset #{offset}")
    # def get_address_data(self, table, page_size, limit_sizelimit_size):
    #     pass

    @Select("select * from #{table} where op_flag=0 or op_flag=1 or op_flag=2 limit #{limit_size}")
    def get_address_data_limit(self, table, limit_size):
        pass

    # @Select("SELECT * FROM ( SELECT t.*, ROWNUM as rn FROM ( SELECT * FROM #{table} ORDER BY id ) t ) WHERE rn BETWEEN #{start_row} AND #{end_row}")
    # def get_address_data_oracle(self, table, start_row, end_row):
    #     pass

    # @Select("select * from #{table} where op_flag!=9 order by id limit #{page_size} offset #{offset}")
    # def get_parsed_data(self, table, page_size, offset):
    #     pass

    @Select("select * from #{table} where op_flag=0 or op_flag=1 or op_flag=2 limit #{limit_size}")
    def get_parsed_data_limit(self, table, limit_size):
        pass

    # @Select("SELECT * FROM ( SELECT t.*, ROWNUM as rn FROM ( SELECT * FROM #{table} ORDER BY id ) t ) WHERE rn BETWEEN #{start_row} AND #{end_row}")
    # def get_parsed_data_oracle(self, table, start_row, end_row):
    #     pass

    @Delete("truncate table #{table}")
    def truncate_table(self, table):
        pass

    @Delete("delete from #{table} where id='#{table_id}'")
    def delete_data(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=9 where id='#{table_id}'")
    def set_completed(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=3 where id='#{table_id}'")
    def set_insert_es_failed(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=9, is_del=0 where id='#{table_id}'")
    def set_notDelete_and_completed(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=9, is_del=1 where id='#{table_id}'")
    def set_delete_and_completed(self, table, table_id):
        pass

    # 无法解析的
    @Update("update #{table} set op_flag=7 where id='#{table_id}'")
    def set_unable_parsed(self, table, table_id):
        pass

    # 把中间状态的更新成完成
    @Update("update #{table} set op_flag=9 where op_flag=8")
    def set_all_waiting_completed(self, table):
        pass

    @Update("update #{table} set op_flag=0 where id='#{table_id}'")
    def set_inserted(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=1 where id='#{table_id}'")
    def set_modified(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=2 where id='#{table_id}'")
    def set_deleted(self, table, table_id):
        pass

    @Update("delete from #{table} where id='#{table_id}'")
    def delete_row(self, table, table_id):
        pass
