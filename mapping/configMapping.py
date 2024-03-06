from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_orm.annoation.dataSourceAnnotation import Select


@Component
class ConfigMapping:
    @Select("select config_name, config_value from s_sysconfig where config_type='address_search'")
    def get_address_search_config(self):
        pass

    @Select("select config_name, config_value from s_sysconfig where config_type='elasticsearch'")
    def get_es_config(self):
        pass

    @Select("select dict_value from #{table}")
    def get_address_dict(self, table):
        pass

    @Select("select sword,tword from #{table}")
    def get_address_thesaurus(self, table):
        pass
