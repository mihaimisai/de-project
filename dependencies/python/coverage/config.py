# Licensed under the Apache License: http://www.apache.org/licenses/LICENSE-2.0
# For details: https://github.com/nedbat/coveragepy/blob/master/NOTICE.txt

"""Config file for coverage.py"""

from __future__ import annotations

import collections
import configparser
import copy
import os
import os.path
import re

from typing import (
    Any, Callable, Final, Mapping, Union,
)
from collections.abc import Iterable

from coverage.exceptions import ConfigError
from coverage.misc import isolate_module, human_sorted_items, substitute_variables
from coverage.tomlconfig import TomlConfigParser, TomlDecodeError
from coverage.types import (
    TConfigurable, TConfigSectionIn, TConfigValueIn, TConfigSectionOut,
    TConfigValueOut, TPluginConfig,
)

os = isolate_module(os)


class HandyConfigParser(configparser.ConfigParser):
    """Our specialization of ConfigParser."""

    def __init__(self, our_file: bool) -> None:
        """Create the HandyConfigParser.

        `our_file` is True if this config file is specifically for coverage,
        False if we are examining another config file (tox.ini, setup.cfg)
        for possible settings.
        """

        super().__init__(interpolation=None)
        self.section_prefixes = ["coverage:"]
        if our_file:
            self.section_prefixes.append("")

    def read( # type: ignore[override]
        self,
        filenames: Iterable[str],
        encoding_unused: str | None = None,
    ) -> list[str]:
        """Read a file name as UTF-8 configuration data."""
        return super().read(filenames, encoding="utf-8")

    def real_section(self, section: str) -> str | None:
        """Get the actual name of a section."""
        for section_prefix in self.section_prefixes:
            real_section = section_prefix + section
            has = super().has_section(real_section)
            if has:
                return real_section
        return None

    def has_option(self, section: str, option: str) -> bool:
        real_section = self.real_section(section)
        if real_section is not None:
            return super().has_option(real_section, option)
        return False

    def has_section(self, section: str) -> bool:
        return bool(self.real_section(section))

    def options(self, section: str) -> list[str]:
        real_section = self.real_section(section)
        if real_section is not None:
            return super().options(real_section)
        raise ConfigError(f"No section: {section!r}")

    def get_section(self, section: str) -> TConfigSectionOut:
        """Get the contents of a section, as a dictionary."""
        d: dict[str, TConfigValueOut] = {}
        for opt in self.options(section):
            d[opt] = self.get(section, opt)
        return d

    def get(self, section: str, option: str, *args: Any, **kwargs: Any) -> str: # type: ignore
        """Get a value, replacing environment variables also.

        The arguments are the same as `ConfigParser.get`, but in the found
        value, ``$WORD`` or ``${WORD}`` are replaced by the value of the
        environment variable ``WORD``.

        Returns the finished value.

        """
        for section_prefix in self.section_prefixes:
            real_section = section_prefix + section
            if super().has_option(real_section, option):
                break
        else:
            raise ConfigError(f"No option {option!r} in section: {section!r}")

        v: str = super().get(real_section, option, *args, **kwargs)
        v = substitute_variables(v, os.environ)
        return v

    def getlist(self, section: str, option: str) -> list[str]:
        """Read a list of strings.

        The value of `section` and `option` is treated as a comma- and newline-
        separated list of strings.  Each value is stripped of white space.

        Returns the list of strings.

        """
        value_list = self.get(section, option)
        values = []
        for value_line in value_list.split("\n"):
            for value in value_line.split(","):
                value = value.strip()
                if value:
                    values.append(value)
        return values

    def getregexlist(self, section: str, option: str) -> list[str]:
        """Read a list of full-line regexes.

        The value of `section` and `option` is treated as a newline-separated
        list of regexes.  Each value is stripped of white space.

        Returns the list of strings.

        """
        line_list = self.get(section, option)
        value_list = []
        for value in line_list.splitlines():
            value = value.strip()
            try:
                re.compile(value)
            except re.error as e:
                raise ConfigError(
                    f"Invalid [{section}].{option} value {value!r}: {e}",
                ) from e
            if value:
                value_list.append(value)
        return value_list


TConfigParser = Union[HandyConfigParser, TomlConfigParser]


# The default line exclusion regexes.
DEFAULT_EXCLUDE = [
    r"#\s*(pragma|PRAGMA)[:\s]?\s*(no|NO)\s*(cover|COVER)",
]

# The default partial branch regexes, to be modified by the user.
DEFAULT_PARTIAL = [
    r"#\s*(pragma|PRAGMA)[:\s]?\s*(no|NO)\s*(branch|BRANCH)",
]

# The default partial branch regexes, based on Python semantics.
# These are any Python branching constructs that can't actually execute all
# their branches.
DEFAULT_PARTIAL_ALWAYS = [
    "while (True|1|False|0):",
    "if (True|1|False|0):",
]


class CoverageConfig(TConfigurable, TPluginConfig):
    """Coverage.py configuration.

    The attributes of this class are the various settings that control the
    operation of coverage.py.

    """
    # pylint: disable=too-many-instance-attributes

    def __init__(self) -> None:
        """Initialize the configuration attributes to their defaults."""
        # Metadata about the config.
        # We tried to read these config files.
        self.config_files_attempted: list[str] = []
        # We did read these config files, but maybe didn't find any content for us.
        self.config_files_read: list[str] = []
        # The file that gave us our configuration.
        self.config_file: str | None = None
        self._config_contents: bytes | None = None

        # Defaults for [run] and [report]
        self._include = None
        self._omit = None

        # Defaults for [run]
        self.branch = False
        self.command_line: str | None = None
        self.concurrency: list[str] = []
        self.context: str | None = None
        self.cover_pylib = False
        self.data_file = ".coverage"
        self.debug: list[str] = []
        self.debug_file: str | None = None
        self.disable_warnings: list[str] = []
        self.dynamic_context: str | None = None
        self.parallel = False
        self.plugins: list[str] = []
        self.relative_files = False
        self.run_include: list[str] = []
        self.run_omit: list[str] = []
        self.sigterm = False
        self.source: list[str] | None = None
        self.source_pkgs: list[str] = []
        self.timid = False
        self._crash: str | None = None

        # Defaults for [report]
        self.exclude_list = DEFAULT_EXCLUDE[:]
        self.exclude_also: list[str] = []
        self.fail_under = 0.0
        self.format: str | None = None
        self.ignore_errors = False
        self.include_namespace_packages = False
        self.report_include: list[str] | None = None
        self.report_omit: list[str] | None = None
        self.partial_always_list = DEFAULT_PARTIAL_ALWAYS[:]
        self.partial_list = DEFAULT_PARTIAL[:]
        self.precision = 0
        self.report_contexts: list[str] | None = None
        self.show_missing = False
        self.skip_covered = False
        self.skip_empty = False
        self.sort: str | None = None

        # Defaults for [html]
        self.extra_css: str | None = None
        self.html_dir = "htmlcov"
        self.html_skip_covered: bool | None = None
        self.html_skip_empty: bool | None = None
        self.html_title = "Coverage report"
        self.show_contexts = False

        # Defaults for [xml]
        self.xml_output = "coverage.xml"
        self.xml_package_depth = 99

        # Defaults for [json]
        self.json_output = "coverage.json"
        self.json_pretty_print = False
        self.json_show_contexts = False

        # Defaults for [lcov]
        self.lcov_output = "coverage.lcov"
        self.lcov_line_checksums = False

        # Defaults for [paths]
        self.paths: dict[str, list[str]] = {}

        # Options for plugins
        self.plugin_options: dict[str, TConfigSectionOut] = {}

    MUST_BE_LIST = {
        "debug", "concurrency", "plugins",
        "report_omit", "report_include",
        "run_omit", "run_include",
    }

    def from_args(self, **kwargs: TConfigValueIn) -> None:
        """Read config values from `kwargs`."""
        for k, v in kwargs.items():
            if v is not None:
                if k in self.MUST_BE_LIST and isinstance(v, str):
                    v = [v]
                setattr(self, k, v)

    def from_file(self, filename: str, warn: Callable[[str], None], our_file: bool) -> bool:
        """Read configuration from a .rc file.

        `filename` is a file name to read.

        `our_file` is True if this config file is specifically for coverage,
        False if we are examining another config file (tox.ini, setup.cfg)
        for possible settings.

        Returns True or False, whether the file could be read, and it had some
        coverage.py settings in it.

        """
        _, ext = os.path.splitext(filename)
        cp: TConfigParser
        if ext == ".toml":
            cp = TomlConfigParser(our_file)
        else:
            cp = HandyConfigParser(our_file)

        self.config_files_attempted.append(os.path.abspath(filename))

        try:
            files_read = cp.read(filename)
        except (configparser.Error, TomlDecodeError) as err:
            raise ConfigError(f"Couldn't read config file {filename}: {err}") from err
        if not files_read:
            return False

        self.config_files_read.extend(map(os.path.abspath, files_read))

        any_set = False
        try:
            for option_spec in self.CONFIG_FILE_OPTIONS:
                was_set = self._set_attr_from_config_option(cp, *option_spec)
                if was_set:
                    any_set = True
        except ValueError as err:
            raise ConfigError(f"Couldn't read config file {filename}: {err}") from err

        # Check that there are no unrecognized options.
        all_options = collections.defaultdict(set)
        for option_spec in self.CONFIG_FILE_OPTIONS:
            section, option = option_spec[1].split(":")
            all_options[section].add(option)

        for section, options in all_options.items():
            real_section = cp.real_section(section)
            if real_section:
                for unknown in set(cp.options(section)) - options:
                    warn(
                        "Unrecognized option '[{}] {}=' in config file {}".format(
                            real_section, unknown, filename,
                        ),
                    )

        # [paths] is special
        if cp.has_section("paths"):
            for option in cp.options("paths"):
                self.paths[option] = cp.getlist("paths", option)
                any_set = True

        # plugins can have options
        for plugin in self.plugins:
            if cp.has_section(plugin):
                self.plugin_options[plugin] = cp.get_section(plugin)
                any_set = True

        # Was this file used as a config file? If it's specifically our file,
        # then it was used.  If we're piggybacking on someone else's file,
        # then it was only used if we found some settings in it.
        if our_file:
            used = True
        else:
            used = any_set

        if used:
            self.config_file = os.path.abspath(filename)
            with open(filename, "rb") as f:
                self._config_contents = f.read()

        return used

    def copy(self) -> CoverageConfig:
        """Return a copy of the configuration."""
        return copy.deepcopy(self)

    CONCURRENCY_CHOICES: Final[set[str]] = {
        "thread", "gevent", "greenlet", "eventlet", "multiprocessing"
    }

    CONFIG_FILE_OPTIONS = [
        # These are *args for _set_attr_from_config_option:
        #   (attr, where, type_="")
        #
        #   attr is the attribute to set on the CoverageConfig object.
        #   where is the section:name to read from the configuration file.
        #   type_ is the optional type to apply, by using .getTYPE to read the
        #       configuration value from the file.

        # [run]
        ("branch", "run:branch", "boolean"),
        ("command_line", "run:command_line"),
        ("concurrency", "run:concurrency", "list"),
        ("context", "run:context"),
        ("cover_pylib", "run:cover_pylib", "boolean"),
        ("data_file", "run:data_file"),
        ("debug", "run:debug", "list"),
        ("debug_file", "run:debug_file"),
        ("disable_warnings", "run:disable_warnings", "list"),
        ("dynamic_context", "run:dynamic_context"),
        ("parallel", "run:parallel", "boolean"),
        ("plugins", "run:plugins", "list"),
        ("relative_files", "run:relative_files", "boolean"),
        ("run_include", "run:include", "list"),
        ("run_omit", "run:omit", "list"),
        ("sigterm", "run:sigterm", "boolean"),
        ("source", "run:source", "list"),
        ("source_pkgs", "run:source_pkgs", "list"),
        ("timid", "run:timid", "boolean"),
        ("_crash", "run:_crash"),

        # [report]
        ("exclude_list", "report:exclude_lines", "regexlist"),
        ("exclude_also", "report:exclude_also", "regexlist"),
        ("fail_under", "report:fail_under", "float"),
        ("format", "report:format"),
        ("ignore_errors", "report:ignore_errors", "boolean"),
        ("include_namespace_packages", "report:include_namespace_packages", "boolean"),
        ("partial_always_list", "report:partial_branches_always", "regexlist"),
        ("partial_list", "report:partial_branches", "regexlist"),
        ("precision", "report:precision", "int"),
        ("report_contexts", "report:contexts", "list"),
        ("report_include", "report:include", "list"),
        ("report_omit", "report:omit", "list"),
        ("show_missing", "report:show_missing", "boolean"),
        ("skip_covered", "report:skip_covered", "boolean"),
        ("skip_empty", "report:skip_empty", "boolean"),
        ("sort", "report:sort"),

        # [html]
        ("extra_css", "html:extra_css"),
        ("html_dir", "html:directory"),
        ("html_skip_covered", "html:skip_covered", "boolean"),
        ("html_skip_empty", "html:skip_empty", "boolean"),
        ("html_title", "html:title"),
        ("show_contexts", "html:show_contexts", "boolean"),

        # [xml]
        ("xml_output", "xml:output"),
        ("xml_package_depth", "xml:package_depth", "int"),

        # [json]
        ("json_output", "json:output"),
        ("json_pretty_print", "json:pretty_print", "boolean"),
        ("json_show_contexts", "json:show_contexts", "boolean"),

        # [lcov]
        ("lcov_output", "lcov:output"),
        ("lcov_line_checksums", "lcov:line_checksums", "boolean")
    ]

    def _set_attr_from_config_option(
        self,
        cp: TConfigParser,
        attr: str,
        where: str,
        type_: str = "",
    ) -> bool:
        """Set an attribute on self if it exists in the ConfigParser.

        Returns True if the attribute was set.

        """
        section, option = where.split(":")
        if cp.has_option(section, option):
            method = getattr(cp, "get" + type_)
            setattr(self, attr, method(section, option))
            return True
        return False

    def get_plugin_options(self, plugin: str) -> TConfigSectionOut:
        """Get a dictionary of options for the plugin named `plugin`."""
        return self.plugin_options.get(plugin, {})

    def set_option(self, option_name: str, value: TConfigValueIn | TConfigSectionIn) -> None:
        """Set an option in the configuration.

        `option_name` is a colon-separated string indicating the section and
        option name.  For example, the ``branch`` option in the ``[run]``
        section of the config file would be indicated with `"run:branch"`.

        `value` is the new value for the option.

        """
        # Special-cased options.
        if option_name == "paths":
            # This is ugly, but type-checks and ensures the values are close
            # to right.
            self.paths = {}
            assert isinstance(value, Mapping)
            for k, v in value.items():
                assert isinstance(v, Iterable)
                self.paths[k] = list(v)
            return

        # Check all the hard-coded options.
        for option_spec in self.CONFIG_FILE_OPTIONS:
            attr, where = option_spec[:2]
            if where == option_name:
                setattr(self, attr, value)
                return

        # See if it's a plugin option.
        plugin_name, _, key = option_name.partition(":")
        if key and plugin_name in self.plugins:
            self.plugin_options.setdefault(plugin_name, {})[key] = value # type: ignore[index]
            return

        # If we get here, we didn't find the option.
        raise ConfigError(f"No such option: {option_name!r}")

    def get_option(self, option_name: str) -> TConfigValueOut | None:
        """Get an option from the configuration.

        `option_name` is a colon-separated string indicating the section and
        option name.  For example, the ``branch`` option in the ``[run]``
        section of the config file would be indicated with `"run:branch"`.

        Returns the value of the option.

        """
        # Special-cased options.
        if option_name == "paths":
            return self.paths

        # Check all the hard-coded options.
        for option_spec in self.CONFIG_FILE_OPTIONS:
            attr, where = option_spec[:2]
            if where == option_name:
                return getattr(self, attr)  # type: ignore[no-any-return]

        # See if it's a plugin option.
        plugin_name, _, key = option_name.partition(":")
        if key and plugin_name in self.plugins:
            return self.plugin_options.get(plugin_name, {}).get(key)

        # If we get here, we didn't find the option.
        raise ConfigError(f"No such option: {option_name!r}")

    def post_process_file(self, path: str) -> str:
        """Make final adjustments to a file path to make it usable."""
        return os.path.expanduser(path)

    def post_process(self) -> None:
        """Make final adjustments to settings to make them usable."""
        self.data_file = self.post_process_file(self.data_file)
        self.html_dir = self.post_process_file(self.html_dir)
        self.xml_output = self.post_process_file(self.xml_output)
        self.paths = {
            k: [self.post_process_file(f) for f in v]
            for k, v in self.paths.items()
        }
        self.exclude_list += self.exclude_also

    def debug_info(self) -> list[tuple[str, Any]]:
        """Make a list of (name, value) pairs for writing debug info."""
        return human_sorted_items(
            (k, v) for k, v in self.__dict__.items() if not k.startswith("_")
        )


def config_files_to_try(config_file: bool | str) -> list[tuple[str, bool, bool]]:
    """What config files should we try to read?

    Returns a list of tuples:
        (filename, is_our_file, was_file_specified)
    """

    # Some API users were specifying ".coveragerc" to mean the same as
    # True, so make it so.
    if config_file == ".coveragerc":
        config_file = True
    specified_file = (config_file is not True)
    if not specified_file:
        # No file was specified. Check COVERAGE_RCFILE.
        rcfile = os.getenv("COVERAGE_RCFILE")
        if rcfile:
            config_file = rcfile
            specified_file = True
    if not specified_file:
        # Still no file specified. Default to .coveragerc
        config_file = ".coveragerc"
    assert isinstance(config_file, str)
    files_to_try = [
        (config_file, True, specified_file),
        ("setup.cfg", False, False),
        ("tox.ini", False, False),
        ("pyproject.toml", False, False),
    ]
    return files_to_try


def read_coverage_config(
    config_file: bool | str,
    warn: Callable[[str], None],
    **kwargs: TConfigValueIn,
) -> CoverageConfig:
    """Read the coverage.py configuration.

    Arguments:
        config_file: a boolean or string, see the `Coverage` class for the
            tricky details.
        warn: a function to issue warnings.
        all others: keyword arguments from the `Coverage` class, used for
            setting values in the configuration.

    Returns:
        config:
            config is a CoverageConfig object read from the appropriate
            configuration file.

    """
    # Build the configuration from a number of sources:
    # 1) defaults:
    config = CoverageConfig()

    # 2) from a file:
    if config_file:
        files_to_try = config_files_to_try(config_file)

        for fname, our_file, specified_file in files_to_try:
            config_read = config.from_file(fname, warn, our_file=our_file)
            if config_read:
                break
            if specified_file:
                raise ConfigError(f"Couldn't read {fname!r} as a config file")

    # 3) from environment variables:
    env_data_file = os.getenv("COVERAGE_FILE")
    if env_data_file:
        config.data_file = env_data_file
    # $set_env.py: COVERAGE_DEBUG - Debug options: https://coverage.rtfd.io/cmd.html#debug
    debugs = os.getenv("COVERAGE_DEBUG")
    if debugs:
        config.debug.extend(d.strip() for d in debugs.split(","))

    # 4) from constructor arguments:
    config.from_args(**kwargs)

    # 5) for our benchmark, force settings using a secret environment variable:
    force_file = os.getenv("COVERAGE_FORCE_CONFIG")
    if force_file:
        config.from_file(force_file, warn, our_file=True)

    # Once all the config has been collected, there's a little post-processing
    # to do.
    config.post_process()

    return config
