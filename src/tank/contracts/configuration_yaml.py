"""YAML configuration admission preserving SafeLoader's declared merge semantics."""

from collections.abc import Hashable

import yaml
from yaml.nodes import MappingNode, ScalarNode


class DuplicateConfigurationKeyError(ValueError):
    """A mapping declares the same constructed key more than once."""


class ConfigurationYamlLoader(yaml.SafeLoader):
    def __init__(self, stream: str | bytes) -> None:
        super().__init__(stream)
        self._checked_mappings: set[MappingNode] = set()

    def flatten_mapping(self, node: MappingNode) -> None:
        if node not in self._checked_mappings:
            seen: set[Hashable] = set()
            for key_node, _value_node in node.value:
                if key_node.tag == "tag:yaml.org,2002:merge":
                    continue
                key: object = (
                    key_node.value
                    if key_node.tag == "tag:yaml.org,2002:value" and isinstance(key_node, ScalarNode)
                    else self.construct_object(key_node)
                )
                if not isinstance(key, Hashable):
                    continue
                if key in seen:
                    raise DuplicateConfigurationKeyError("YAML explicit mapping keys must be unique")
                seen.add(key)
            self._checked_mappings.add(node)
        super().flatten_mapping(node)
