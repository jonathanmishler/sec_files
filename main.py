from timeit import default_timer as timer
import logging
import psycopg
import db
import sec_financials

logging.basicConfig(level=logging.INFO)

db.models.Num.__table__.drop(db.setup.engine)
db.models.Pre.__table__.drop(db.setup.engine)
db.models.Tag.__table__.drop(db.setup.engine)
db.models.Sub.__table__.drop(db.setup.engine)

db.setup.main_setup()
sec = sec_financials.SecFiles()

with psycopg.connect(db.settings.Settings().conn_str) as conn:
    with conn.cursor() as cur:
        t1 = timer()
        logging.info(f"Filling Sub Table")
        db.ops.copy_to_table(cur, db.models.Sub, sec.sub)
        t2 = timer()
        logging.info(f"Sub table took {(t2-t1)/60} min to fill")
        logging.info(f"Filling Tag Table")
        db.ops.insert_records(db.models.Tag, sec.tag, True)
        t3 = timer()
        logging.info(f"Tag table took {(t3-t2)/60} min to fill")
        logging.info(f"Filling Num Table")
        db.ops.copy_to_table(cur, db.models.Num, sec.num)
        t4 = timer()
        logging.info(f"Num table took {(t4-t3)/60} min to fill")
        logging.info(f"Filling Pre Table")
        db.ops.copy_to_table(cur, db.models.Pre, sec.pre)
        t5 = timer()
        logging.info(f"Pre table took {(t5-t4)/60} min to fill")