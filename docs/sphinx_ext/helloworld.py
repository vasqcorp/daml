from docutils import nodes
from docutils.parsers.rst import Directive


class HelloWorld(Directive):

    def run(self):
        paragraph_node = nodes.paragraph(text='Hello World xD ala123!')
        return [paragraph_node]


def setup(app):
    app.add_directive("helloworld123", HelloWorld)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }