from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Select, Delete, Update


@Component
class AddressMapping:
    @Select("select dict_value from #{table}")
    def get_address_dict(self, table):
        pass

    @Select("select sword,tword from #{table}")
    def get_address_thesaurus(self, table):
        pass

    @Select("select id, address as fullname, x, y, op_flag, is_del from #{table} where op_flag!=9 order by id limit #{page_size} offset #{offset}")
    def get_address_data(self, table, page_size, offset):
        pass

    @Select("select * from #{table} where op_flag!=9 order by id limit #{page_size} offset #{offset}")
    def get_parsed_data(self, table, page_size, offset):
        pass

    @Delete("truncate table #{table}")
    def truncate_table(self, table):
        pass

    @Select("select count(1) from #{table} where op_flag!=9")
    def get_data_count(self, table):
        pass

    @Delete("delete from #{table} where id=#{table_id}")
    def delete_data(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=9 where id=#{table_id}")
    def set_completed(self, table, table_id):
        pass

    # 分页过程中更新成op_flag=9会导致分页查询有问题，因为查的是 op_flag !=9的，所以先更新成中间状态，即 op_flag=8
    @Update("update #{table} set op_flag=8 where id=#{table_id}")
    def set_waiting_completed(self, table, table_id):
        pass

    # 分页过程中更新成op_flag=9会导致分页查询有问题，因为查的是 op_flag !=9的，所以先更新成中间状态，即 op_flag=8
    @Update("update #{table} set op_flag=8, is_del=0 where id=#{table_id}")
    def set_notDelete_and_waiting_completed(self, table, table_id):
        pass

    # 分页过程中更新成op_flag=9会导致分页查询有问题，因为查的是 op_flag !=9的，所以先更新成中间状态，即 op_flag=8
    @Update("update #{table} set op_flag=8, is_del=1 where id=#{table_id}")
    def set_delete_and_waiting_completed(self, table, table_id):
        pass

    # 把中间状态的更新成完成
    @Update("update #{table} set op_flag=9 where op_flag=8")
    def set_all_waiting_completed(self, table):
        pass

    @Update("update #{table} set op_flag=0 where id=#{table_id}")
    def set_inserted(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=1 where id=#{table_id}")
    def set_modified(self, table, table_id):
        pass

    @Update("update #{table} set op_flag=2 where id=#{table_id}")
    def set_deleted(self, table, table_id):
        pass

