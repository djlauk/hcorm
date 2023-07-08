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
    print_php_header(model, f)
    print_php_db_helpers(model, f)
    print_php_gateway_classes(model, f)


def print_php_header(model: DataModel, f=sys.stdout):
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


def print_php_db_helpers(model: DataModel, f=sys.stdout):
    f.write(
        r"""
// ---------- db helpers ----------

$_db_debug = false;

class VersionMismatchError extends \Exception { }


class AmbiguousQueryError extends \Exception { }


function _db_helper_enableDebug($enabled = false) {
    global $_db_debug;
    $_db_debug = $enabled;
}


// because "LIMIT :offset, :pagesize" will not work
function _db_helper_limitClause($offset=0, $pagesize=50) {
    if (!is_int($offset)) {
        throw new Exception("offset must be integer");
    }
    if ($offset < 0) {
        throw new Exception("offset must be greater or equals 0");
    }
    if (!is_int($pagesize)) {
        throw new Exception("pagesize must be integer");
    }
    if ($pagesize < 1) {
        throw new Exception("pagesize must be greater 0");
    }
    return "LIMIT ${offset}, ${pagesize}";
}


function _db_helper_stopWithError($errorInfo = null) {
    global $_db_debug;
    $msg = "Error during DB access";
    if ($_db_debug && !is_null($errorInfo)) {
        $msg .= "\n\nDEBUG INFO:\n" . implode("\n", $errorInfo);
    }
    die($msg);
}


/**
 * _db_helper_query will execute $sql in a prepared statement and return all rows as an array.
 */
function _db_helper_query(&$pdo, $sql, $values = null) {
    $statement = $pdo->prepare($sql);
    if ($statement === false) {
        _db_helper_stopWithError($pdo->errorInfo());
    }
    if ($statement->execute($values) !== true) {
        _db_helper_stopWithError($statement->errorInfo());
    }

    $results = $statement->fetchAll();
    if ($results === false) {
        _db_helper_stopWithError($statement->errorInfo());
    }
    return $results;
}


/**
 * _db_helper_querySingle will execute $sql in a prepared statement and expects 0 or 1 results.
 * If more than 1 result is returned an AmbiguousQueryError is thrown.
 */
function _db_helper_querySingle(&$pdo, $sql, $values = null) {
    $results = _db_helper_query($pdo, $sql, $values);
    $count = count($results);
    if ($count === 0) {
        return null;
    }
    if ($count > 1) {
        throw new AmbiguousQueryError("Query returned $count results");
    }
    return $results[0];
}


/**
 * _db_helper_execute will execute the SQL in $sql in a prepared statement.
 */
function _db_helper_execute(&$pdo, $sql, $values = null) {
    $statement = $pdo->prepare($sql);
    if ($statement === false) {
        _db_helper_stopWithError($pdo->errorInfo());
    }
    if ($statement->execute($values) !== true) {
        _db_helper_stopWithError($statement->errorInfo());
    }
}


/**
 * _db_helper_insert executes the SQL statement in $sql and returns the Id of the last insert.
 *
 * @return int Id which the last insert genertated.
 */
function _db_helper_insert(&$pdo, $sql, $values = null) {
    _db_helper_execute($sql, $values);
    return $pdo->lastInsertId();
}

"""
    )


def print_php_gateway_classes(model: DataModel, f=sys.stdout):
    f.write(
        """
// ---------- gateway classes for tables ----------

"""
    )
    for tname in model.get_tablenames_sorted():
        print_php_gateway_class(tname, model, f)


def print_php_gateway_class(tname: str, model: DataModel, f=sys.stdout):
    f.write(f"/** gateway class for table {tname} */\n")
    classname = php_name_for_table(tname)
    f.write(f"class {classname} {{\n")

    tbl = model.tables[tname]
    fieldnames = [php_name_for_column(x) for x in tbl.columns]
    for cname, fieldname in zip(tbl.columns, fieldnames):
        f.write(f"\tpublic ${fieldname} = null;\n")
    f.write("\n")

    f.write("\tpublic function toArray() {\n")
    f.write("\t\treturn array(\n")
    for cname, fieldname in zip(tbl.columns, fieldnames):
        f.write(f"\t\t\t'{cname}' => $this->{fieldname},\n")
    f.write("\t\t);\n")
    f.write("\t}\n\n")

    f.write("\tpublic function fromArray($arr) {\n")
    for cname, fieldname in zip(tbl.columns, fieldnames):
        f.write(f"\t\t$this->{fieldname} = $arr['{cname}'] ?? $this->{fieldname};\n")
    f.write("\t}\n\n")

    f.write(
        f"""\tpublic static function createFromArray($arr) {{
\t\t$obj = new {classname}();
\t\t$obj->fromArray($arr);
\t\treturn $obj;
\t}}\n\n"""
    )

    f.write("\tprivate static function _select_snippet() {\n")
    select_fields = ",\n  ".join([f"`{tname}`.`{cname}`" for cname in tbl.columns])
    f.write(
        f"""\t\t$sql = <<<HERE
SELECT
{select_fields}
FROM `{tname}`
HERE;
"""
    )
    f.write("\t\treturn $sql;\n")
    f.write("\t}\n\n")

    f.write("\t// ---------- CRUD operations ----------\n\n")

    f.write("\tpublic static function dbCount(&$pdo) {\n")
    f.write(f"\t\t$sql = 'SELECT COUNT(*) FROM `{tname}`';\n")
    f.write("\t}\n\n")

    f.write(
        "\tpublic static function dbLoadWhere(&$pdo, $values=null, $offset=0, $pagesize=10) {\n"
    )
    f.write(f"\t\t$sql = {classname}::_select_snippet();\n")
    f.write("\t\tif (!is_null($values) && count($values) > 0) {\n")
    f.write("\t\t\t$parts = array();\n")
    f.write("\t\t\tforeach($values as $k => $v) {\n")
    f.write(f"""\t\t\t\t$parts[] = "(`{tname}`.`${{k}}` = :${{k}})";\n""")
    f.write("\t\t\t}\n")
    f.write("\t\t\t$sql .= ' WHERE ' . implode(' AND ', $parts);\n")
    f.write("\t\t}\n")
    f.write("\t\t$sql .= ' ' . _db_helper_limitClause($offset, $pagesize);\n")
    f.write("\t\t$dbresults = _db_helper_query($pdo, $sql, $values);\n")
    f.write(f"\t\t$arr = array_map('{classname}::createFromArray', $dbresults);\n")
    f.write("\t\treturn $arr;\n")
    f.write("\t}\n\n")

    f.write("\tpublic static function dbList(&$pdo, $offset=0, $pagesize=10) {\n")
    f.write(f"\t\t$arr = {classname}::dbLoadWhere($pdo, null, $offset, $pagesize);\n")
    f.write("\t\treturn $arr;\n")
    f.write("\t}\n\n")

    pkvars = ["$" + php_name_for_column(x) for x in tbl.primary_key]
    value_array = ", ".join(
        [
            f"'{php_name_for_column(x)}' => ${php_name_for_column(x)}"
            for x in tbl.primary_key
        ]
    )
    f.write(
        f"\tpublic static function dbLoadByPrimaryKey(&$pdo, {', '.join(pkvars)}) {{\n"
    )
    f.write(f"\t\t$values = array({value_array});\n")
    f.write(f"\t\t$results = {classname}::dbLoadWhere($pdo, $values);\n")
    f.write("\t\t$count = count($results);\n")
    f.write("\t\tif ($count === 0) return null;\n")
    f.write(
        f"\t\tif ($count > 1) throw new AmbiguousQueryError('{classname}::dbLoadByPrimaryKey found more than 1 entry');\n"
    )
    f.write(f"\t\t$obj = $results[0];\n")
    f.write("\t\treturn $obj;\n")
    f.write("\t}\n\n")

    f.write("\tpublic function dbInsert(&$pdo) {\n")
    f.write("\t}\n\n")

    f.write("\tpublic function dbUpdate(&$pdo) {\n")
    f.write("\t}\n\n")

    f.write("\tpublic function dbUpsert(&$pdo) {\n")
    f.write("\t}\n\n")

    f.write("\tpublic function dbDelete(&$pdo) {\n")
    f.write("\t}\n\n")

    f.write("\t// ---------- navigating relationships ----------\n\n")

    if len(tbl.foreign_keys) == 0:
        f.write("\t// table has no foreign keys\n\n")

    for fk in tbl.foreign_keys:
        f.write(
            f"\tpublic function dbLoadAllFor{fk.ref_table}(&$pdo, ${php_name_for_column(fk.column)}) {{\n"
        )
        f.write(
            f"\t\t$sql = {classname}::_select_snippet() . ' WHERE `{tname}`.`{fk.column}` = :{fk.column}';\n"
        )
        f.write(
            f"\t\t$values = array('{fk.column}' => ${php_name_for_column(fk.column)});\n"
        )
        f.write("\t\t$dbresults = _db_helper_query($pdo, $sql, $values);\n")
        f.write(f"\t\t$arr = array_map('{classname}::createFromArray', $dbresults);\n")
        f.write("\t\treturn $arr;\n")
        f.write("\t}\n\n")

    # end of class
    f.write("}\n\n")


def php_name_for_table(tname: str) -> str:
    # noop for now
    return tname


def php_name_for_column(cname: str) -> str:
    # noop for now
    return cname


if __name__ == "__main__":
    cli()
