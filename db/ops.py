from typing import Type, Iterable, Optional, List
import logging
from pydantic import fields
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
import psycopg
from psycopg import sql
from sqlmodel import Session, SQLModel
from .setup import engine


def gen_data_model(
    model: Type[SQLModel],
    data: Iterable,
    exclude: Optional[dict] = None,
    as_dict: bool = False,
):
    """Generator returning each row of the parsed data model with the option
    to return as a dict or tuple with excludable fields.  export_type can be
    model, dict, or tuple
    """
    for row in data:
        row = model.parse_obj(row)
        if as_dict:
            row = row.dict(exclude=exclude)
        yield row


def pg_insert_stmt(model: Type[SQLModel], records: List[dict]):
    """Using sqlalchemy's postgres dialect support, create the insert statement
    with conflict resolution
    """
    return insert(model).values(records).on_conflict_do_nothing()


def exclude_auto_pk_fields(model):
    """Returns a dict of the primary key that will be auto generated in the database"""
    primary_keys = [pk.name for pk in inspect(model).primary_key]
    # If primary key is optional, then it should be set to auto generate
    req_fields = model.schema(by_alias=False)["required"]
    return {field: True for field in primary_keys if field not in req_fields}


def insert_records(model: Type[SQLModel], data: Iterable, skip_conflict: bool = False):
    """Inserts the data into the database and if it is a postgres database we can use the
    conflict resolution to skip records which already exsist in our table"""

    if skip_conflict:
        logging.info("Using Postgres Dialect to skip on conflict")
        exclude_fields = exclude_auto_pk_fields(model)
        as_dict = True
    elif not skip_conflict:
        logging.info("Using standard sqlalchemy method")
        exclude_fields = None
        as_dict = False
    records = list()
    i = 0
    with Session(engine) as session:
        for record in gen_data_model(model, data, exclude_fields, as_dict):
            if record:
                if skip_conflict:
                    records.append(record)
                elif not skip_conflict:
                    session.add(record)
                i += 1
                if i == 10000:
                    if skip_conflict:
                        session.execute(pg_insert_stmt(model, records))
                        records.clear()
                    session.commit()
                    i = 0
        if i > 0:
            if skip_conflict:
                session.execute(pg_insert_stmt(model, records))
            session.commit()


def copy_to_table(cursor: psycopg.Cursor, model: Type[SQLModel], data: Iterable):
    exclude_fields = exclude_auto_pk_fields(model)
    copy_fields = [
        field_name
        for field_name in model.__fields__.keys()
        if field_name not in exclude_fields.keys()
    ]
    stmt = sql.SQL("COPY {table_name} ({fields}) FROM STDIN").format(
        table_name=sql.Identifier(model.__tablename__),
        fields=sql.SQL(",").join(
            [sql.Identifier(field_name) for field_name in copy_fields]
        ),
    )
    logging.info(stmt.as_string(cursor))
    with cursor.copy(stmt) as copy:
        logging.info(copy)
        for record in gen_data_model(model, data, exclude_fields, True):
            copy.write_row([record.get(field_name) for field_name in copy_fields])
