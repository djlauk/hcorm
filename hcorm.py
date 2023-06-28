#!/usr/bin/env python

# This file is part of hcorm. hcorm is licensed under the MIT license.
# See LICENSE.md for details.

from dataclasses import dataclass
from time import strftime
from typing import Any, Dict, Generic, List, Optional, Sequence, TypeVar
import sys
import graphlib

import click
import yaml


T = TypeVar("T")


class CaseInsensitiveLookup(Generic[T]):
    """A case-insensitive lookup similar to a ``dict``."""

    def __init__(self) -> None:
        self._d: Dict[str, T] = {}
        self._keys: List[str] = []

    def __contains__(self, item: str) -> bool:
        return item.lower() in self._d

    def __delitem__(self, k: str) -> T:
        raise NotImplementedError()

    def __getitem__(self, k: str) -> T:
        return self._d[k.lower()]

    def __setitem__(self, k: str, v: T) -> None:
        if k.lower() in self:
            raise ValueError(f"key has already been set: {k}")
        self._keys.append(k)
        self._d[k.lower()] = v

    def __iter__(self):
        return self._keys.__iter__()

    def get(self, k: str, default: Optional[T] = None) -> Optional[T]:
        return self._d.get(k.lower(), default)

    def keys(self) -> Sequence[str]:
        """Obtain the keys in the order they were added. Case from the first addition is preserved."""
        return self._keys[:]


@dataclass
class DbColumn:
    name: str
    db_type: str


@dataclass
class DbForeignKey:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class DbTable:
    name: str
    columns: CaseInsensitiveLookup[DbColumn]
    primary_key: List[str]
    foreign_keys: List[DbForeignKey]


@dataclass
class DataModel:
    tables: CaseInsensitiveLookup[DbTable]
    typealiases: CaseInsensitiveLookup[str]
    columnsets: CaseInsensitiveLookup[CaseInsensitiveLookup[DbColumn]]

    def get_tablenames_sorted(self):
        """Get the table names in a topologically sorted order."""
        toposort = graphlib.TopologicalSorter()
        for tname in self.tables:
            tbl = self.tables[tname]
            deps = [self.tables[fk.ref_table].name for fk in tbl.foreign_keys]
            toposort.add(tbl.name, *deps)
        return toposort.static_order()


@click.group()
def cli():
    pass


@click.group()
def generate():
    pass


@click.command()
@click.option("-m", "--model-file")
def generatesql(model_file):
    model = model_from_yaml(model_file)
    print_sql(model)


@click.command()
@click.option("-m", "--model-file")
def generatephp(model_file):
    model = model_from_yaml(model_file)
    print_php(model)


@click.command()
@click.option("-m", "--model-file")
def checkmodel(model_file):
    model = model_from_yaml(model_file)
    nerrors = 0

    # check 1: check foreign keys for existing references
    for tname in model.tables:
        tbl = model.tables[tname]
        for fk in tbl.foreign_keys:
            if fk.column not in tbl.columns:
                print(
                    f"ERROR: table {tname} references non-existing column in foreign key: {fk.column}"
                )
                nerrors += 1
                continue
            if fk.ref_table not in model.tables:
                print(
                    f"ERROR: table {tname} references non-existing table: {fk.ref_table}"
                )
                nerrors += 1
                continue
            reftbl = model.tables[fk.ref_table]
            if fk.ref_column not in reftbl.columns:
                print(
                    f"ERROR: table {tname} references non-existing column in table {fk.ref_table}: {fk.ref_column}"
                )
                nerrors += 1
                continue

    # check 2: check for cycles in table references
    try:
        _ = model.get_tablenames_sorted()
    except graphlib.CycleError as err:
        print(f"ERROR: cyclic dependencies in tables found: {err}")
        nerrors += 1
    except KeyError as err:
        # silently ignore, this has been handled in check 1
        pass

    # summary
    print(f"number of errors found: {nerrors}")


cli.add_command(checkmodel)
cli.add_command(generatephp)
cli.add_command(generatesql)


def model_from_yaml(fname: str) -> DataModel:
    with open(fname, "r") as f:
        data = yaml.safe_load(f)
    model = build_data_model(data)
    return model


def build_data_model(d: Dict[str, Any]) -> DataModel:
    typealiases = build_typealiases(d.get("typealiases", {}))
    columnsets = build_columnsets(d.get("columnsets", {}), typealiases)
    if "tables" not in d:
        raise ValueError("no tables defined in YAML file")
    tables = build_tables(d["tables"], typealiases, columnsets)
    model = DataModel(typealiases=typealiases, columnsets=columnsets, tables=tables)

    return model


def build_typealiases(d: Dict[str, str]) -> CaseInsensitiveLookup[str]:
    aliases = CaseInsensitiveLookup()
    for k, v in d.items():
        aliases[k] = v
    return aliases


def build_columnsets(
    d: Dict[str, List[Dict[str, str]]], typealiases: CaseInsensitiveLookup[str]
) -> CaseInsensitiveLookup[CaseInsensitiveLookup[DbColumn]]:
    csets = CaseInsensitiveLookup()
    for k, v in d.items():
        try:
            csets[k] = build_columnset(v, typealiases)
        except ValueError as err:
            raise ValueError(f"columnset '{k}' invalid: {err}")
    return csets


def build_columnset(
    li: List[Dict[str, str]], typealiases: CaseInsensitiveLookup[str]
) -> CaseInsensitiveLookup[DbColumn]:
    cset = CaseInsensitiveLookup()
    for d in li:
        col = build_column(d, typealiases)
        cset[col.name] = col
    return cset


def build_column(
    d: Dict[str, str], typealiases: CaseInsensitiveLookup[str]
) -> DbColumn:
    if "name" not in d:
        raise ValueError(f"column name missing")
    if "type" not in d and "typealias" not in d:
        raise ValueError(f"column type or typealias missing")
    n = d["name"]
    t = d["type"] if "type" in d else typealiases[d["typealias"]]
    col = DbColumn(name=n, db_type=t)
    return col


def build_tables(
    d: Dict[str, Dict[str, Any]],
    typealiases: CaseInsensitiveLookup[str],
    columnsets: CaseInsensitiveLookup[CaseInsensitiveLookup[DbColumn]],
) -> CaseInsensitiveLookup[DbTable]:
    tables = CaseInsensitiveLookup()
    for k, v in d.items():
        try:
            tables[k] = build_table(k, v, typealiases, columnsets)
        except ValueError as err:
            raise ValueError(f"table '{k}' invalid: {err}")
    return tables


def build_table(
    name: str,
    d: Dict[str, Any],
    typealiases: CaseInsensitiveLookup[str],
    columnsets: CaseInsensitiveLookup[CaseInsensitiveLookup[DbColumn]],
) -> DbTable:
    columns = build_columnset(d["columns"], typealiases)
    # extend columns with column sets
    for csetname in d.get("columnsets", []):
        cset = columnsets[csetname]
        for cname in cset:
            columns[cname] = cset[cname]

    primary_key = d["primarykey"]
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    for c in primary_key:
        if c not in columns:
            raise ValueError(f"primary key references non-existing column: {c}")

    foreign_keys = [build_foreignkey(x) for x in d.get("foreignkeys", [])]
    tbl = DbTable(
        name=name, columns=columns, primary_key=primary_key, foreign_keys=foreign_keys
    )
    return tbl


def build_foreignkey(d: Dict[str, str]) -> DbForeignKey:
    if "column" not in d:
        raise ValueError(f"column name missing in foreign key")
    if "reftable" not in d:
        raise ValueError(f"reftable missing in foreign key")
    if "refcolumn" not in d:
        raise ValueError(f"refcolumn missing in foreign key")
    fk = DbForeignKey(
        column=d["column"], ref_table=d["reftable"], ref_column=d["refcolumn"]
    )
    return fk


def print_sql(model: DataModel, f=sys.stdout):
    ts = strftime(r"%Y-%m-%d %H:%M:%S")
    f.write(
        f"""-- ----------------------------------------------------------------------
-- hcorm generated database structure
-- for details on hcorm see https://github.com/djlauk/hcorm
--
-- generated on {ts}
-- ----------------------------------------------------------------------

"""
    )
    for tname in model.get_tablenames_sorted():
        tbl = model.tables[tname]
        f.write(f"CREATE TABLE `{tname}` (\n")
        for cname in tbl.columns:
            col = tbl.columns[cname]
            print(f"  `{col.name}` {col.db_type},")

        f.write("\n")
        f.write(f'  PRIMARY KEY ({", ".join([f"`{c}`" for c in tbl.primary_key])})')
        for fk in tbl.foreign_keys:
            f.write(",\n")
            f.write(
                f"  FOREIGN KEY (`{fk.column}`) REFERENCES `{fk.ref_table}` (`{fk.ref_column}`)"
            )
        f.write("\n);\n\n")


def print_php(model: DataModel, f=sys.stdout):
    ts = strftime(r"%Y-%m-%d %H:%M:%S")
    f.write(
        f"""<?php
//----------------------------------------------------------------------
// hcorm generated DB gateway classes
// for details on hcorm see https://github.com/djlauk/hcorm
//
// generated on {ts}
// ----------------------------------------------------------------------

"""
    )
    for tname in model.get_tablenames_sorted():
        f.write(f"/** gateway class for table {tname} */\n")
        classname = php_name_for_table(tname)
        f.write(f"class {classname}")
        f.write("{\n")

        tbl = model.tables[tname]
        for cname in tbl.columns:
            fieldname = php_name_for_column(cname)
            f.write(f"\t${fieldname};\n")

        f.write("}\n\n")


def php_name_for_table(tname: str) -> str:
    # noop for now
    return tname


def php_name_for_column(cname: str) -> str:
    # noop for now
    return cname


if __name__ == "__main__":
    cli()
