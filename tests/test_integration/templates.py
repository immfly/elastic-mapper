from elastic_mapper import templates as elastic_templates
from elastic_mapper import parsers


class TestStringTemplate(elastic_templates.Template):
    name = "test_template_string"
    index = "test-string-{time}"
    parser = parsers.MonthlyParser()


class TestIntTemplate(elastic_templates.Template):
    name = "test_template_int"
    index = "test-int-{time}"
    parser = parsers.MonthlyParser()
