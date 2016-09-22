from elastic_mapper import templates as elastic_templates
from elastic_mapper import parsers


class TestTemplateApp2(elastic_templates.Template):
    name = "test_template_app2"
    index = "test-app2-{time}"
    parser = parsers.DailyParser()

    class Meta:
        number_of_shards = 1
