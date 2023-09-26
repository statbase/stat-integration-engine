from sqlalchemy.orm import Session
from sqlalchemy.sql import and_, or_, case, else
from . import schemas


def get_all_tags(db: Session) -> dict:
    tag_counts = {}
    all_tag_lists = db.query(schemas.DataBlock.tags).all()
    for tag_list in all_tag_lists:
        for tag in tag_list.split(';'):
            if tag in tag_counts.keys():
                tag_counts[tag] += 1
            else:
                tag_counts[tag] = 1
    return dict(sorted(tag_counts.items(), key=lambda count: count[1], reverse=True))


def get_datablocks(db: Session, search_term: str = '', tags: list[str] = None, **filters):
    datablock_col = schemas.DataBlock
    q = db.query(datablock_col).filter(or_(datablock_col.title.__contains__(search_term),
                                           datablock_col.tags.__contains__(search_term)))
    if filters:
        q = q.filter_by(**filters)
    if tags:
        tags_col = schemas.DataBlock.tags
        filter_conditions = [tags_col.__contains__(tag) for tag in tags]
        filter_query = and_(*filter_conditions)
        q = q.filter(filter_query)
    q = q.order_by(
        case(
        [
                    (datablock_col.tags.__contains__(tags),1)
                ],
            else_=2
        )
    )
    q.limit(100)
    return q.all()

def get_geo_list(db: Session):
    q = db.query(schemas.GeoUnit).order_by(schemas.GeoUnit.geo_id)
    return q.all()
