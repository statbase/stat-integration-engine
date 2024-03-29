import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_, or_, case, delete
from sqlalchemy.dialects.sqlite import insert

from models import models
from database import schemas


""" READ """


def get_tags(db: Session, tag_list: list[str] = None) -> dict:
    tags_col = schemas.DataBlock.tags
    tag_counts = {}
    q = db.query(schemas.DataBlock.tags)
    if tag_list:
        filter_conditions = [tags_col.like(f'%{tag}%') for tag in tag_list]
        q = q.filter(and_(*filter_conditions))
    tag_list_list = q.all()
    for tag_list in tag_list_list:
        for tag in tag_list[0].split(';'):
            if tag in tag_counts.keys():
                tag_counts[tag] += 1
            else:
                tag_counts[tag] = 1
    return dict(sorted(tag_counts.items(), key=lambda count: count[1], reverse=True))


# noinspection PyTypeChecker
def get_datablocks(db: Session, search_term: str = '', tags: list[str] = None,
                   row_limit: int = 0,  **filters) -> list[models.DataBlock]:
    schema_datablock = schemas.DataBlock
    q = db.query(schema_datablock).filter(or_(schema_datablock.title.like(f'%{search_term}%'),
                                              schema_datablock.tags.like(f'%{search_term}%')))
    if filters:
        q = q.filter_by(**filters)

    if tags:
        tags_col = schemas.DataBlock.tags
        filter_conditions = [tags_col.like(f'%{tag}%') for tag in tags]
        filter_query = and_(*filter_conditions)
        q = q.filter(filter_query)

    q = q.order_by(
        case(
            (schema_datablock.title.like(search_term), 1),
            else_=2
        )
    )
    if int(row_limit) > 0:
        q = q.limit(row_limit)
    rows = q.all()

    return [models.DataBlock(**row.__dict__) for row in rows]


def get_geo_list(db: Session):
    q = db.query(schemas.GeoUnit).order_by(schemas.GeoUnit.geo_id)
    return q.all()


def get_timeseries(db: Session, data_id: int,
                   geo_list: list[str]) -> tuple[pd.DataFrame, list[str]]:
    ts_col = schemas.Timeseries
    q = db.query(ts_col).filter(ts_col.data_id == data_id)
    if geo_list:
        q = q.filter(ts_col.geo_id.in_(geo_list))
    df = pd.read_sql(q.statement, q.session.bind)

    # TODO: figure out more efficient way to do this
    found_geo_ids = set(df['geo_id'].tolist())
    missing_geo_ids = [geo_id for geo_id in geo_list if geo_id not in found_geo_ids]

    return df, missing_geo_ids


def calculate_meta(db: Session, data_id) -> dict:
    ts = schemas.Timeseries

    def calculate_span(row_list: list[str]) -> str:
        span_str = row_list[0][0] + '-' + row_list[-1][0]
        return span_str

    def calculate_resolution(row_list: list) -> str:
        return 'year'

    def calculate_var_values(row_list: list) -> list[str]:
        return [row[0] for row in row_list]

    date_query = (
        db.query(ts.date)
        .filter(ts.data_id == data_id)
        .distinct()
        .order_by(ts.date)
    )
    date_rows = date_query.all()

    if not date_rows:
        raise FileNotFoundError('No timeseries found')

    meta = {'span': calculate_span(date_rows),
            'resolution': calculate_resolution(date_rows)}

    var_query = (
        db.query(ts.variable)
        .filter(ts.data_id == data_id)
        .distinct()
    )
    var_rows = var_query.all()
    meta['var_values'] = calculate_var_values(var_rows)

    return meta


""" WRITE """


def upsert_datablocks(db: Session, datablock_list: list[models.SourceDataBlock]):
    for dblock in datablock_list:
        q = insert(schemas.DataBlock).values(**dblock.__dict__)
        q = q.on_conflict_do_update(
            index_elements=['source_id'],
            set_={
                'title': q.excluded.title,
                'type': q.excluded.type,
                'source': q.excluded.source,
                'tags': q.excluded.tags,
                'var_labels': q.excluded.var_labels,
                'geo_groups': q.excluded.geo_groups,
                'description': q.excluded.description,
            }
        )
        db.execute(q)
    db.commit()


def insert_timeseries(db: Session, df: pd.DataFrame):
    if len(df) == 0:
        return
    df.to_sql(con=db.bind, name='timeseries', if_exists='append', index=False)


def update_datablock_meta(db: Session, data_id: int, meta: dict):
    (db.query(schemas.DataBlock).
     filter(schemas.DataBlock.data_id == data_id).
     update({'meta': meta}))
    db.commit()


def clear_timeseries_for_integration(db: Session, integration_id: int):
    ts = schemas.Timeseries
    dblock = schemas.DataBlock
    delete_stmt = delete(ts).where(ts.data_id.in_(
        db.query(dblock.data_id).filter(dblock.integration_id == integration_id)
    ))
    db.execute(delete_stmt)
    db.commit()
