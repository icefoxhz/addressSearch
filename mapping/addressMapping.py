from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Select, Delete, Update


@Component
class AddressMapping:
    @Select("select id, address as fullname, x, y, flag from #{table} where flag!=9 limit #{page_size} offset #{offset}")
    def get_address_data(self, table, page_size, offset):
        pass

    @Select("select * from #{table} where flag!=9 limit #{page_size} offset #{offset}")
    def get_parsed_data(self, table, page_size, offset):
        pass

    @Delete("truncate table #{table}")
    def truncate_table(self, table):
        pass

    @Select("select count(1) from #{table} where flag!=9")
    def get_data_count(self, table):
        pass

    @Delete("delete from #{table} where id=#{table_id}")
    def delete_data(self, table, table_id):
        pass

    @Update("update #{table} set flag=9 where id=#{table_id}")
    def set_completed(self, table, table_id):
        pass

    @Update("update #{table} set flag=0 where id=#{table_id}")
    def set_inserted(self, table, table_id):
        pass

    @Update("update #{table} set flag=1 where id=#{table_id}")
    def set_modified(self, table, table_id):
        pass

    @Update("update #{table} set flag=2 where id=#{table_id}")
    def set_deleted(self, table, table_id):
        pass

