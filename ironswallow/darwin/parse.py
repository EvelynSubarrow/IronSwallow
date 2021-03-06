import io, xml.sax, re
from collections import OrderedDict
from typing import Union, Optional, Tuple
import traceback

from ironswallow.darwin import kb_consts


DARWIN_PATHS = ("Pport.uR", "Pport.uR.schedule", "Pport.uR.TS", "Pport.uR.OW",
                "Pport.sR", "Pport.sR.schedule", "Pport.sR.TS", "Pport.sR.OW",
                "PportTimetableRef", "PportTimetableRef.LateRunningReasons", "PportTimetableRef.CancellationReasons"
                )
DARWIN_NAMED_LIST_PATHS = (
    'Pport.uR.scheduleFormations.formation.coaches.coach', 'Pport.sR.scheduleFormations.formation.coaches.coach',
    'Pport.sR.scheduleFormations.formation', 'Pport.uR.scheduleFormations.formation'
)
DARWIN_DETOKENISE = ("Pport.uR.OW.Msg", "Pport.sR.OW.Msg")


def parse_darwin_suppress(cm_pair) -> Tuple[int, Union[list, str, None]]:
    count, message = cm_pair
    message_decoded = "-"
    try:
        if message:
            message_decoded = message.decode("utf8")
            return count, parse_darwin(message)
    except Exception as e:
        return count, message_decoded + "".join(traceback.format_stack())
    return count, None


def parse_darwin(message) -> Optional[list]:
    if message:
        message_decoded = message.decode("utf8")
        parsed = DarwinParser(DARWIN_PATHS, DARWIN_DETOKENISE, folded_list=DARWIN_NAMED_LIST_PATHS).parse(io.StringIO(message_decoded))["Pport"]
        return (parsed.get("uR", {}) or parsed.get("sR", {})).get("list", [])


def parse_kb(text) -> dict:
    return DarwinParser(include_tags=False, folded_list=kb_consts.FOLD_LISTS, exclude_data=kb_consts.EXCLUDE_DATA, collapse_data=kb_consts.FLAT_DATA, collapse_data_types=kb_consts.DATA_TYPES).parse(io.StringIO(text))


def parse_xml(message) -> dict:
    return DarwinParser(DARWIN_PATHS, DARWIN_DETOKENISE).parse(io.StringIO(message.decode("utf8")))


def _coerce_bool(text) -> bool:
    if text.lower() == "true":
        return True
    elif text.lower() == "false":
        return False
    else:
        raise ValueError("Key marked as bool type but not a boolean: " + text)



class DarwinParser(xml.sax.ContentHandler):
    _TYPE_REGEXES = [ (re.compile(k),v) for k,v in
        [(r"^[+-]?\d+\.\d+$", float),
        (r"^[+-]?\d+$", int),
        (r"^(?:true|false)$", bool),
        (r".*", str)]
    ]

    def __init__(self, list_paths=(), detokenise=(), folded_list=(), exclude_data=(), collapse_data=(), collapse_data_types=(), exclude_keys=(), strip_whitespace=True, include_tags=True, profile=False):
        self._path = []
        self._root = OrderedDict()
        self._dicts = [self._root]
        # lists are O(n), sets are less
        self._list_paths = [a.split(".") for a in list_paths]
        self._folded_list = set(folded_list)
        self._exclude_data = set(exclude_data)
        self._collapse_data = set(collapse_data)
        self._exclude_keys = set(exclude_keys)
        self._collapse_types = dict(collapse_data_types)

        for key,type_ in list(self._collapse_types.items()):
            if type_ == bool:
                self._collapse_types[key] = _coerce_bool

        self._collision_paths = []
        self._data_path_status = {}
        self._data_path_count = {}
        self._data_enum = {}
        self._all_paths = set()
        self._detokenise = detokenise
        self._strip_whitespace = strip_whitespace
        self._include_tags = include_tags
        self._profile = profile
        self._exclude_key_trigger = False

    def startElement(self, name, attrs) -> None:
        name = name.split(":")[-1]
        current_path = ".".join(self._path)

        if current_path in self._detokenise:
            # rewrite tag back to text
            self.characters("<{}{}{}>".format(name, " "*bool(len(attrs)), " ".join(['{}="{}"'.format(k, v) for k, v in attrs.items()])))
        else:
            self._path.append(name)
            new_path = ".".join(self._path)

            element_struct = OrderedDict()
            if self._include_tags:
                element_struct["tag"] = name
            element_struct.update([(k, v) for k, v in attrs.items() if not k.startswith("xmlns")])

            if self._profile:
                #TODO: allow excluding attributes, include them here
                self._all_paths |= {new_path}

            if self._exclude_key_trigger:
                pass
            elif new_path in self._exclude_keys:
                self._exclude_key_trigger = True
            elif "list" in self._dicts[-1]:
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

            if new_path not in self._collapse_data and new_path not in self._exclude_keys and not self._exclude_key_trigger:
                self._dicts.append(element_struct)

            if self._path in self._list_paths:
                element_struct["list"] = []

    def endElement(self, name) -> None:
        name = name.split(":")[-1]
        current_path = ".".join(self._path)

        if current_path in self._exclude_keys:
            self._exclude_key_trigger = False
            self._path.pop()
        elif self._exclude_key_trigger:
            self._path.pop()
        elif ".".join(self._path) in self._detokenise and not self._path[-1] == name:
            self.characters("</{}>".format(name))
        elif current_path in self._collapse_data:
            contents = self._dicts[-1][self._path[-1]]

            if current_path in self._collapse_types:
                # coerce type
                self._dicts[-1][self._path[-1]] = self._collapse_types[current_path](contents)
            elif self._profile:

                # slightly silly layout for this because it means you'll need a second pass for typing. sorry.
                if self._profile and contents and current_path not in self._list_paths and current_path not in self._folded_list:
                    if not self._data_enum.get(current_path): self._data_enum[current_path] = set()
                    self._data_enum[current_path] |= {next(iter([v for k, v in self._TYPE_REGEXES if k.match(contents.rstrip())]))}

            # Pop path (because that is set) but not dict (because this isn't a dict!!)
            self._path.pop()
        else:
            if self._profile:
                self._data_path_count[current_path] = self._data_path_count.get(current_path, False) or list(self._dicts[-1].keys())!=["$"]

            self._path.pop()
            self._dicts.pop()

    def characters(self, data) -> None:
        full_path = ".".join(self._path)

        if full_path in self._exclude_data or self._exclude_key_trigger:
            pass
        elif full_path in self._collapse_data:
            # We're writing back directly to our name
            if full_path in self._folded_list:
                self._dicts[-1][self._path[-1]][-1] += data
            else:
                self._dicts[-1][self._path[-1]] += data
        else:
            # it's going in '$' I guess
            if "$" not in self._dicts[-1]:
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
            self._data_enum = {k: list(v)[0] for k,v in self._data_enum.items() if v != {str} and len(v)==1}
        return self._root
