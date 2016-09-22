from elastic_mapper import templates as elastic_templates
from elastic_mapper import parsers


class TestTemplateApp1(elastic_templates.Template):
    name = "test_template_app1"
    index = "test-app1-{time}"
    parser = parsers.MonthlyParser()

    class Meta:
        number_of_shards = 1
