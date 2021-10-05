import json
from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.parsers.rst import directives
from sphinx.locale import _
from sphinx.writers.html5 import HTML5Translator
from collections import defaultdict
import re
import os

ref_data = {}
metrics_data = {}
error_codes_data = {}

CONFIG_OPT = 'embed_reference_json'

def load_data(app, config):
    global ref_data
    global metrics_data
    global error_codes_data

    print("ALA123 :: ")
    print(config)
    print("|" + config[CONFIG_OPT] + "|")
    # print(config["aaaaaa"])
    # for k, v in config.items():
    #     print("ALA123 :: ")
    #     print("Key: ")
    #     print(k)
    #     print(" value:")
    #     print(v)

    if CONFIG_OPT in config:
        file_name = config[CONFIG_OPT]

        try:
            with open(file_name) as f:
                tmp = json.load(f)
                # ref_data = tmp["console"]
                # metrics_data = tmp["metrics"]

                # building a map [error_code -> error_information]
                error_codes_data = {error["code"]: error for error in tmp["errorCodes"]}
        except EnvironmentError:
            print("Failed to open file: %s" % file_name)
            raise


def setup(app):
    app.add_config_value(CONFIG_OPT, '', 'env')
    app.connect('config-inited', load_data)

    # app.add_node(console_topic)
    # app.add_node(metric_section)
    app.add_node(error_code_node)
    # We use these directives as entry points for where to put the console/metric references for a particular topic
    # i.e. in a `.rst` file we can now do '..console-top :: Health' to add the console documentation for Health commands
    # app.add_directive('console-topic', ConsoleTopicDirective)
    # app.add_directive('metric-section', MetricSectionDirective)
    # app.add_directive('overwrite-error-code-explanation-or-resolution', OverwriteErrorCodeExplOrResDirective)
    app.add_directive('list-all-error-codes', ListAllErrorCodesDirective)

    # Callback functions for populating the console and metric sections after the doctree is resolved
    # app.connect('doctree-resolved', process_topic_nodes)
    # app.connect('doctree-resolved', process_metric_nodes)
    # app.connect('doctree-resolved', process_error_code_nodes)

    # Overwriting standard Sphinx translator to allow linking of return types
    app.set_translator('html', PatchedHTMLTranslator)


class console_topic(nodes.General, nodes.Element):

    def __init__(self, topic):
        nodes.Element.__init__(self)
        print("found topic: %s" % topic)
        self.topic = topic

class metric_section(nodes.General, nodes.Element):

    def __init__(self, section):
        nodes.Element.__init__(self)
        print("found metric section: %s" % section)
        self.section = section

class error_code_node(nodes.General, nodes.Element):
    def __init__(self, codes, expl="", res=""):
        nodes.Element.__init__(self)
        print("found error node codes: %s" % codes)
        self.codes = codes
        self.expl = expl
        self.res = res

class ConsoleTopicDirective(Directive):

    has_contents = False
    required_arguments = 1
    final_argument_whitespace = True

    def run(self):
        # get all topics given to this directive in the form topic1,topic2,...
        topic = [x.strip() for x in self.arguments[0].split(',')]
        return [console_topic(topic)]

class MetricSectionDirective(Directive):

    has_contents = False
    required_arguments = 1
    final_argument_whitespace = True

    def run(self):
        section = self.arguments[0].strip()
        return [metric_section(section)]

class ListAllErrorCodesDirective(Directive):
    has_contents = True
    required_arguments = 0
    final_argument_whitespace = True
    def run(self):
        return [error_code_node([], None, None)]

class OverwriteErrorCodeExplOrResDirective(Directive):

    has_contents = True
    required_arguments = 1
    final_argument_whitespace = True
    # see https://docutils.sourceforge.io/docs/howto/rst-directives.html
    option_spec = {
        'resolution': directives.unchanged,
        'explanation': directives.unchanged
    }

    def run(self):
        # get all errors codes given in the form .. error-codes:: code1,code2,...
        codes = [x.strip() for x in self.arguments[0].split(',')]
        assert len(self.arguments) == 1, "You are only allowed to give exactly 1 argument to this custom-directive"
        options = defaultdict(str, self.options)
        expl, res = options['explanation'], options['resolution']
        return [error_code_node(codes, expl, res)]

def text_node(n, txt):
    # Doubling the parameter, as TextElements want the raw text and the text
    return n(txt, txt)

def process_topic_nodes(app, doctree, fromDocName):

    # bail if there is a command which has no topic
    invalid = list(filter(lambda x : len(x['topic']) == 0, ref_data))
    if len(invalid) > 0:
        raise Exception("commands without topic " + str(invalid))

    # keep track of all topics
    topics = set(map(lambda x : ",".join(x['topic']), ref_data))
    # assumption is that the console macro is only used in a single rst file
    # i.e. we assume that all topics will be picked up by that file
    have = False

    problems = []

    for node in doctree.traverse(console_topic):
        have = True

        def wrapped_summary(i):
            if i["scope"] == "Stable":
                return i["summary"]
            return "(%s) %s" % (i["scope"], i["summary"])

        def item_to_node(item):
            node = nodes.definition_list_item()
            term = item['name']
            term_node = text_node(nodes.term, term)
            summary = text_node(nodes.paragraph, wrapped_summary(item))
            description = text_node(nodes.paragraph, item['description'])
            arguments_heading = text_node(nodes.paragraph, 'Arguments:')
            arguments = nodes.bullet_list()
            def arg_item(arg_type):
                item = nodes.list_item()
                item += text_node(nodes.literal, '%s: %s' % (arg_type[0], arg_type[1]))
                return item

            arguments += [build_node_with_link_to_scala_docs(arg_item(arg), arg[1]) for arg in item['arguments']]
            return_type_heading = text_node(nodes.paragraph, 'Return type')
            return_type_text_node = text_node(nodes.literal, item['return_type'])
            return_type_node = build_node_with_link_to_scala_docs(return_type_text_node, item['return_type'])

            definition_node = nodes.definition('', summary)
            # Create a permalink to the term_node
            permalink_node = build_permalink(app, fromDocName, item['name'], 'usermanual/console', term_node)
            if(item['arguments']):
                definition_node += [arguments_heading, arguments]
            if(item['return_type']):
                definition_node += [return_type_heading, return_type_node]
            if(item['description']):
                definition_node += description
            node += [permalink_node, definition_node]
            return node

        # If return type is a Canton type, create hyperlink to its scaladocs-page
        # This function could be made arbitrarily complex/clever -> but this already covers 95% of cases
        def build_node_with_link_to_scala_docs(return_type_text_node, type_string):
            # A 'complex type' is e.g. Option[Boolean]
            type_is_complex_type = re.search('\[(.*)\]', type_string)
            # grab type inside the '[...]' if it is 'complex'
            type_string = type_string if not type_is_complex_type else type_is_complex_type.group(1)

            # don't link return type
            if not is_canton_type(type_string) or '[' in type_string: return return_type_text_node

            # generated scaladocs links work only for website/release (not locally)
            website_root = '../../scaladoc/'
            ci_unidoc_root = '/tmp/workspace/docs/scaladoc'
            local_unidoc_root = 'target/scala-2.13/unidoc'

            type_path = build_type_path(type_string)

            if os.environ.get("CHECK_DOCS_LINKS_TO_SCALADOC") and not os.path.exists(ci_unidoc_root + '/' + type_path + '.html'):
                print(type_path + " does not exist in the build unidoc. fix me by adding a manual special case")
                return return_type_text_node
            return_type_node = nodes.reference('', '', refuri=website_root + type_path + '.html')
            return_type_node += return_type_text_node
            return return_type_node

        def build_type_path(type_string):
            type_path = type_string.replace('.', '/')
            if is_special_case_type(type_path):
                return generate_special_case_type_url(type_path)
            else:
                return type_path

        def is_special_case_type(type_path):
            special_cases = ['ListShareRequestsResponse', 'ListShareOffersResponse', 'ContractData', 'VersionInfo',
                             'LfContractId', '=>', 'WrappedCreatedEvent', 'SequencerConnectionConfigs', 'SequencerCounter']
            return any([s in type_path for s in special_cases])

        # Add manual special cases here
        def generate_special_case_type_url(type_path):
            if 'ListShareRequestsResponse' in type_path or 'ListShareOffersResponse' in type_path:
                # actual: canton/participant/admin/v0/ListShareRequestsResponse/Item.html
                # should: canton/participant/admin/v0/ListShareRequestsResponse.html
                return type_path.replace('/Item', '')
            elif 'ContractData' in type_path:
                # actual: canton/admin/api/client/commands/LedgerApiTypeWrappers/ContractData.html
                # should: canton/admin/api/client/commands/LedgerApiTypeWrappers$$ContractData.html
                return type_path.replace('/ContractData', '$$ContractData')
            elif 'VersionInfo' in type_path:
                # actual: canton/buildinfo/Info/VersionInfo.html
                # should: canton/buildinfo/Info$$VersionInfo
                return type_path.replace('/VersionInfo', '$$VersionInfo')
            elif 'LfContractId' in type_path:
                # special cases of LfContractId, LfContractId* and Map[...LfContractId,String]
                # should: canton/protocol/index.html
                idx = type_path.find('LfContractId')
                return type_path[:idx] + 'index'
            elif 'SequencerConnectionConfigs' in type_path:
                # actual: canton/sequencing/SequencerConnectionConfigs.html
                # should: canton/buildinfo/index.html
                return type_path.replace('SequencerConnectionConfigs', 'index')
            elif '=>' in type_path:
                if 'WrappedCreatedEvent' in type_path:  # this is a 'double' special case
                    # actual: canton/admin/api/client/commands/LedgerApiTypeWrappers/WrappedCreatedEvent => Boolean.html
                    # should: canton/admin/api/client/commands/LedgerApiTypeWrappers$$WrappedCreatedEvent.html
                    return type_path.replace('/WrappedCreatedEvent => Boolean', '$$WrappedCreatedEvent')
                else:
                    # case of 'normal' function arrow
                    return type_path[:type_path.find(' => ')]
            elif 'WrappedCreatedEvent' in type_path:
                # actual: canton/admin/api/client/commands/LedgerApiTypeWrappers/WrappedCreatedEvent.html
                # should: canton/admin/api/client/commands/LedgerApiTypeWrappers$$WrappedCreatedEvent.html
                return type_path.replace('/WrappedCreatedEvent', '$$WrappedCreatedEvent')
            elif 'SequencerCounter' in type_path:
                # actual: canton/SequencerCounter.html
                # should: canton/index.html
                idx = type_path.find('SequencerCounter')
                return type_path[:idx] + 'index'
            else:
                raise Exception("Unable to match type in special case handling")

        topic_nodes = list(map(item_to_node, sorted([i for i in ref_data if i['topic'] == node.topic], key = lambda i: i['name'])))

        if len(topic_nodes) == 0:
            problems.append("no commands for topic " + str(node.topic))
        else:
            topics.remove(",".join(node.topic))
            dlist = nodes.definition_list()
            for topic_node in topic_nodes:
                dlist += topic_node
            node.replace_self([dlist])

    # check that we didn't hit issues
    if len(problems) > 0:
        raise Exception(str(problems))

    # check that we picked up all topics
    if len(topics) > 0 and have:
        raise Exception("the following topics are missing from our documentation " + str(topics))


def is_canton_type(type_string):
    return type_string.split(".")[:2] in [['com', 'digitalasset']]

def process_metric_nodes(app, doctree, fromDocName):

    for node in doctree.traverse(metric_section):

        def item_to_node(item):
            node = nodes.definition_list_item()
            term_node = text_node(nodes.term, item["name"])
            summary = text_node(nodes.paragraph, "(%s): %s" % (item["type"], item["summary"]))
            description = text_node(nodes.paragraph, item['description'])
            definition_node = nodes.definition('', summary)
            definition_node += description
            # Create an anchor to the node term_node
            permalink_node = build_permalink(app, fromDocName, item['name'], 'usermanual/monitoring', term_node)
            node += [permalink_node, definition_node]
            return node

        section_items = metrics_data[node.section]
        section_nodes = list(map(item_to_node, section_items))
        dlist = nodes.definition_list()
        for snode in section_nodes:
            dlist += snode
        node.replace_self([dlist])


def process_error_code_nodes(app, doctree, fromDocName):
    def build_indented_bold_and_non_bold_node(bold_text, non_bold_text):
        bold = text_node(nodes.strong, bold_text)
        non_bold = text_node(nodes.inline, non_bold_text)
        both = nodes.definition('', bold)
        both += non_bold
        return both

    def item_to_node(item):
        node = nodes.definition_list_item()
        term_node = text_node(nodes.term, "%s" % (item["code"]))

        definition_node = nodes.definition('', text_node(nodes.paragraph, ''))
        if item["explanation"]:
            definition_node += build_indented_bold_and_non_bold_node("Explanation: ", item['explanation'])
        definition_node += build_indented_bold_and_non_bold_node("Category: ", item['category'])
        if item["conveyance"]:
            definition_node += build_indented_bold_and_non_bold_node("Conveyance: ", item['conveyance'])
        # Create an anchor to the node term_node
        if item["resolution"]:
            definition_node += build_indented_bold_and_non_bold_node("Resolution: ", item['resolution'])
        scaladoc_node = build_node_with_link_to_scala_docs(text_node(nodes.term, ""), item["className"])
        scaladoc_indented = build_indented_bold_and_non_bold_node("Scaladoc", "")
        scaladoc_node += scaladoc_indented
        definition_node += scaladoc_node
        permalink_node = build_permalink(app, fromDocName, item["code"], 'usermanual/error_codes', term_node)
        node += [permalink_node, definition_node]

        return node

    def build_node_with_link_to_scala_docs(return_type_text_node, type_string):
        # generated scaladocs links work only for website/release (not locally)
        website_root = '../../scaladoc/'
        ci_unidoc_root = '/tmp/workspace/docs/scaladoc'
        local_unidoc_root = 'target/scala-2.13/unidoc'

        possible_type_paths = generate_possible_type_paths(type_string)
        type_path = [type_path for type_path in possible_type_paths if os.path.exists(ci_unidoc_root + '/' + type_path + '.html')]
        if os.environ.get("CHECK_DOCS_LINKS_TO_SCALADOC"):
            if len(type_path) != 1:
                raise Exception("Couldn't generate the URL to link to the scaladoc of error code '%s'. This shouldn't happen so talk to Arne if this exception occurs" % (type_string))
            type_path = type_path[0]
        else:
            type_path = ""

        return_type_node = nodes.reference('', '', refuri=website_root + type_path + '.html')
        return_type_node += return_type_text_node
        return return_type_node

    def generate_possible_type_paths(type_string):
        type_path = type_string.replace('.', '/')

        # example input: a$b$
        # example output: ['a$b$', 'a$$b$', 'a$b$$', 'a$$b$$']
        # Generates all 2^n combinations of using in $ or $$ for all occurrences of $ in a string
        # Needed to 'brute-force' the correct scaladoc url for error codes
        # because e.g. the type_string that we that we get through scala reflection is:
        # TransactionRoutingError$$TopologyErrors$$NotConnectedToAllContractDomains$
        # but the name of the corresponding scaladoc `.html`-file that we need is:
        # TransactionRoutingError$$TopologyErrors$$NotConnectedToAllContractDomains$
        # I found no way to deterministically construct the correct html-file name we need -> brute-force approach
        # An exception will be thrown if this brute-force approach ever fails
        def generate_possible_urls(type_string):
            prefixs = [[]]
            for c in type_string:
                if c == "$":
                    prefixs_with_dollar = [prefix + ["$"] for prefix in prefixs]
                    prefixs.extend(prefixs_with_dollar)
                [prefix.append(c) for prefix in prefixs]
            return ["".join(pre) for pre in prefixs]

        return generate_possible_urls(type_path)

    # A node of this tree is a dict that can contain
    #   1. further nodes and/or
    #   2. 'leaves' in the form of a list of error (code) data
    # Thus, the resulting tree is very similar to a trie
    def build_hierarchical_tree_of_error_data(data):
        create_node = lambda: defaultdict(create_node)
        root = defaultdict(create_node)
        for error_data in data:
            current = root
            for group in error_data["hierarchicalGrouping"]:
                current = current[group]
            if 'error-codes' in current:
                current['error-codes'].append(error_data)
            else:
                current['error-codes'] = [error_data]
        return root

    # DFS to traverse the error code data tree from `build_hierarchical_tree_of_error_data`
    # While traversing the tree, the presentation of the error codes on the documentation is built
    def dfs(tree, node, prefix):
        if 'error-codes' in tree:
            dlist = nodes.definition_list()
            for code in tree['error-codes']:
                dlist += item_to_node(code)
            node += dlist
        i = 1
        for (subtopic, subtree) in tree.items():
            if subtopic == 'error-codes': continue
            subprefix = prefix + "%s." % (i)
            i += 1
            subtree_node = text_node(nodes.rubric, subprefix + " " + subtopic)

            dfs(subtree, subtree_node, subprefix)
            node += subtree_node

    for node in doctree.traverse(error_code_node):
        # Valid error codes given to the .. error-codes:: directive as argument
        given_error_codes = [error_codes_data[code] for code in node.codes if code in error_codes_data]
        # Code for manually overwriting the explanation or resolution of an error code
        if (node.expl or node.res) and len(given_error_codes) > 0:
            print((node.expl, node.res))
            assert len(given_error_codes) == 1, "If post-hoc overwriting the explanation or resolution of an error code," \
                                     " you are only allowed to call the directive '.. _error-codes:' with 1 error code" \
                                     "while here you had: " + str(given_error_codes)
            error = given_error_codes[0]
            if node.expl: error["explanation"] = node.expl
            if node.res: error["resolution"] = node.res
            # need to 'self-destruct' here as sphinx otherwise doesn't know how to render the node
            node.replace_self([])
        else:  # list all error codes
            section = nodes.section()
            root = nodes.rubric("", "")
            section += root
            tree = build_hierarchical_tree_of_error_data(error_codes_data.values())
            dfs(tree, root, "")
            node.replace_self([section])


# Build a permalink/anchor to a specific command/metric
def build_permalink(app, fromDocName, term, docname, node_to_permalink_to):
    reference_node = nodes.reference('', '')
    reference_node['refuri'] = app.builder.get_relative_uri(fromDocName, docname) + '#' + term

    reference_node += node_to_permalink_to

    target_node = nodes.target('', '', ids=[term])
    node_to_permalink_to += target_node
    return reference_node




class PatchedHTMLTranslator(HTML5Translator):
    # We overwrite this method as otherwise an assertion fails whenever we create a reference whose parent is
    # not a TextElement. Concretely, this enables using method `build_return_type_node` for creating links from
    # return types to the appropriate scaladocs
    # Similar to solution from https://stackoverflow.com/a/61669375
    def visit_reference(self, node):
        atts = {'class': 'reference'}
        if node.get('internal') or 'refuri' not in node:
            atts['class'] += ' internal'
        else:
            atts['class'] += ' external'
        if 'refuri' in node:
            atts['href'] = node['refuri'] or '#'
            if self.settings.cloak_email_addresses and atts['href'].startswith('mailto:'):
                atts['href'] = self.cloak_mailto(atts['href'])
                self.in_mailto = True
        else:
            assert 'refid' in node, \
                'References must have "refuri" or "refid" attribute.'
            atts['href'] = '#' + node['refid']
        if not isinstance(node.parent, nodes.TextElement):
            # ---------------------
            # Commenting out this assertion is the only change compared to Sphinx version 3.4.3
            # assert len(node) == 1 and isinstance(node[0], nodes.image)
            # ---------------------
            atts['class'] += ' image-reference'
        if 'reftitle' in node:
            atts['title'] = node['reftitle']
        if 'target' in node:
            atts['target'] = node['target']
        self.body.append(self.starttag(node, 'a', '', **atts))

        if node.get('secnumber'):
            self.body.append(('%s' + self.secnumber_suffix) %
                             '.'.join(map(str, node['secnumber'])))