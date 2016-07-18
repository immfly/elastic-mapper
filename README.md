# elastic-mapper
Library and Toolkit to easily export metrics to ES and manage templates and mappings

Example: 

```python
class TestTemplate(Template):
    name = "test_template"
    template = "test-*"
    
    class Meta:
    	# settings
        number_of_shards = 1


class NestedMapper(Mapper):
    nested_string = StringField()
    nested_int_with_source = IntegerField(source='nested_int_field')


@register('test_type', TestTemplate)
class TestMapper(Mapper):
    string_field = StringField()
    int_field = IntegerField()
    string_field_with_source = StringField(source='another_attr')
    method_field = StringField(source='mapper__get_test_method_field')
    nested_field = NestedMapper()

    def get_test_method_field(self, obj):
    	return "test method string"
```
