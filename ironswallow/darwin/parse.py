import io, xml.sax
from collections import OrderedDict
from typing import Optional

DARWIN_PATHS = ("Pport.uR", "Pport.uR.schedule", "Pport.uR.TS", "Pport.uR.OW", "PportTimetableRef", "PportTimetableRef.LateRunningReasons", "PportTimetableRef.CancellationReasons")
DARWIN_DETOKENISE = "Pport.uR.OW.Msg"

KB_PATHS = ("StationList",)

def parse_darwin(message) -> Optional[dict]:
    if message:
        return DarwinParser(DARWIN_PATHS, DARWIN_DETOKENISE).parse(io.StringIO(message.decode("utf8")))["Pport"].get("uR", {})


def parse_kb(text) -> dict:
    return DarwinParser(KB_PATHS, include_tags=False).parse(io.StringIO(text))


def parse_xml(message) -> dict:
    return DarwinParser(DARWIN_PATHS, DARWIN_DETOKENISE).parse(io.StringIO(message.decode("utf8")))


class DarwinParser(xml.sax.ContentHandler):
    def __init__(self, list_paths=(), detokenise="<PLACEHOLDER>", folded_list=(), exclude_data=(), collapse_data=(), strip_whitespace=True, include_tags=True, profile=False):
        self._path = []
        self._root = OrderedDict()
        self._dicts = [self._root]
        # lists are O(n), sets are less
        self._list_paths = [a.split(".") for a in list_paths]
        self._folded_list = set(folded_list)
        self._exclude_data = set(exclude_data)
        self._collapse_data = set(collapse_data)

        self._collision_paths = []
        self._data_path_status = {}
        self._data_path_count = {}
        self._detokenise = detokenise
        self._strip_whitespace = strip_whitespace
        self._include_tags = include_tags
        self._profile = profile

    def startElement(self, name, attrs) -> None:
        name = name.split(":")[-1]
        current_path = ".".join(self._path)

        if current_path.startswith(self._detokenise):
            # rewrite tag back to text
            self.characters("<{}{}{}>".format(name, " "*bool(len(attrs)), " ".join(['{}="{}"'.format(k, v) for k, v in attrs.items()])))
        else:
            self._path.append(name)
            new_path = ".".join(self._path)

            element_struct = OrderedDict()
            if self._include_tags:
                element_struct["tag"] = name
            element_struct.update([(k, v) for k, v in attrs.items() if not k.startswith("xmlns")])

            if "list" in self._dicts[-1]:
                # this is the classic and very silly way of dealing with lists
                # TODO: convert ironswallow to be less silly
                self._dicts[-1]["list"].append(element_struct)
            elif new_path in self._folded_list:
                # This is the much more sensible way
                if name not in self._dicts[-1]:
                    self._dicts[-1][name] = []
                if new_path in self._collapse_data:
                    self._dicts[-1][name].append("")
                else:
                    self._dicts[-1][name].append(element_struct)
            elif new_path in self._collapse_data:
                self._dicts[-1][name] = ""
            else:
                if self._profile:
                    if name in self._dicts[-1]:
                        if new_path not in self._collision_paths:
                            self._collision_paths.append(new_path)
                self._dicts[-1][name] = element_struct

            if new_path not in self._collapse_data:
                self._dicts.append(element_struct)

            if self._path in self._list_paths:
                element_struct["list"] = []

    def endElement(self, name) -> None:
        name = name.split(":")[-1]
        current_path = ".".join(self._path)

        if ".".join(self._path).startswith(self._detokenise) and not self._path[-1]==name:
            self.characters("</{}>".format(name))
        elif current_path in self._collapse_data:
            # Pop path (because that is set) but not dict (because this isn't a dict!!)
            self._path.pop()
        else:
            if self._profile:
                self._data_path_count[current_path] = self._data_path_count.get(current_path, False) or list(self._dicts[-1].keys())!=["$"]

            self._path.pop()
            self._dicts.pop()

    def characters(self, data) -> None:
        full_path = ".".join(self._path)

        if full_path in self._collapse_data:
            # We're writing back directly to our name
            if full_path in self._folded_list:
                self._dicts[-1][self._path[-1]][-1] += data
            else:
                self._dicts[-1][self._path[-1]] += data
        else:
            # it's going in '$' I guess
            if "$" not in self._dicts[-1] and full_path not in self._exclude_data:
                self._dicts[-1]["$"] = ""
                if self._profile:
                    if full_path not in self._data_path_status:
                        self._data_path_status[full_path] = False
            if (not data.isspace() and not self._dicts[-1]["$"].isspace()) or not self._strip_whitespace:
                self._dicts[-1]["$"] += data
                if self._profile:
                    self._data_path_status[full_path] = True

    def parse(self, f) -> dict:
        xml.sax.parse(f, self)
        if self._profile:
            self._data_path_status = [k for k, v in self._data_path_status.items() if not v]
            self._data_path_count = [k for k, v in self._data_path_count.items() if not v]
        return self._root
