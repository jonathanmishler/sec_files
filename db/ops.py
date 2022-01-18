from typing import Type, Iterable, Optional, List
import logging
from pydantic import fields
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect
import psycopg
from psycopg import sql
from sqlmodel import Session, SQLModel
from .setup import engine


def gen_data_model(
    model: Type[SQLModel],
    data: Iterable,
    as_dict: bool = False,
    exclude: Optional[dict] = None
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


def insert_stmt(model: Type[SQLModel], skip_conflit: bool = False):
    """Using sqlalchemy's postgres dialect support, create the insert statement
    with conflict resolution
    """
    if skip_conflit:
        stmt = postgresql.insert(model).on_conflict_do_nothing()
    else:
        stmt = model.__table__.insert()
    
    return stmt


def exclude_auto_pk_fields(model):
    """Returns a dict of the primary key that will be auto generated in the database"""
    primary_keys = [pk.name for pk in inspect(model).primary_key]
    # If primary key is optional, then it should be set to auto generate
    req_fields = model.schema(by_alias=False)["required"]
    return {field: True for field in primary_keys if field not in req_fields}


def insert_records(model: Type[SQLModel], data: Iterable, skip_conflict: bool = False):
    """Inserts the data into the database and if it is a postgres database we can use the
    conflict resolution to skip records which already exsist in our table"""

    exclude_fields = exclude_auto_pk_fields(model)
    records = list()
    i = 0
    stmt = insert_stmt(model, skip_conflict)
    with Session(engine) as session:
        for record in gen_data_model(model, data, True, exclude_fields):
            if record:
                records.append(record)
                i += 1

            if i == 10000:
                session.execute(stmt, records)
                session.commit()
                records.clear()
                i = 0
        if i > 0:
            session.execute(stmt, records)
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
