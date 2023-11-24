from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Select, Delete


@Component
class AddressMapping:
    @Select("select id, standardaddress as fullname, x, y from #{table} limit #{page_size} offset #{offset}")
    def get_address_data(self, table, page_size, offset):
        pass

    @Select("select * from #{table} limit #{page_size} offset #{offset}")
    def get_parsed_data(self, table, page_size, offset):
        pass

    @Delete("truncate table  #{table}")
    def truncate_table(self, table):
        pass

    @Select("select count(1) from #{table}")
    def get_data_count(self, table):
        pass
