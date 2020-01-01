import io, xml.sax
from collections import OrderedDict

class PushPortParser(xml.sax.ContentHandler):
    def __init__(self, list_paths=["Pport.uR", "Pport.uR.schedule", "Pport.uR.TS"], strip_whitespace=True):
        self._path = []
        self._root = OrderedDict()
        self._dicts = [self._root]
        self._list_paths = [a.split(".") for a in list_paths]
        self._strip_whitespace = strip_whitespace

    def startElement(self, name, attrs):
        name = name.split(":")[-1]
        self._path.append(name)

        element_struct = OrderedDict((("tag", name),))
        element_struct.update([(k,v) for k,v in attrs.items() if not k.startswith("xmlns")])

        if "list" in self._dicts[-1]:
            self._dicts[-1]["list"].append(element_struct)
        else:
            self._dicts[-1][name] = element_struct

        self._dicts.append(element_struct)

        if self._path in self._list_paths:
            element_struct["list"] = []

    def endElement(self, name):
        name = name.split(":")[-1]

        self._path.pop()
        self._dicts.pop()

    def characters(self, data):
        if "$" not in self._dicts[-1]:
            self._dicts[-1]["$"] = ""
        if (not data.isspace() and not self._dicts[-1]["$"].isspace()) or not self._strip_whitespace:
            self._dicts[-1]["$"] += data

    def parse(self, f):
        xml.sax.parse(f, self)
        return self._root
