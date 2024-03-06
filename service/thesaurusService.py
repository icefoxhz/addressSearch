from pySimpleSpringFramework.spring_core.type.annotation.classAnnotation import Component
from pySimpleSpringFramework.spring_core.type.annotation.methodAnnotation import Autowired, Value

from addressSearch.mapping.addressMapping import AddressMapping


@Component
class ThesaurusService:
    """
    同义词字典
    """

    @Value({
        "project.tables.thesaurus_table": "_thesaurus_table",
    })
    def __init__(self):
        self._thesaurus_table = None
        self._addressMapping = None
        self.s2t = {}
        self.t2s = {}

    @Autowired
    def set_params(self, addressMapping: AddressMapping):
        self._addressMapping = addressMapping

    def after_init(self):
        df = self._addressMapping.get_address_thesaurus(self._thesaurus_table)
        if df is None or df.empty:
            return

        for index, row in df.iterrows():
            s = row['sword']
            t = row['tword']

            if s not in self.s2t.keys():
                self.s2t[s] = [t]
            else:
                self.s2t[s].append(t)

            if t not in self.t2s.keys():
                self.t2s[t] = [s]
            else:
                self.t2s[t].append(s)

