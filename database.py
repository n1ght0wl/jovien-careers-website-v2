from sqlalchemy import create_engine, text
import os

#print(sqlalchemy.__version__)

db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string,
                       connect_args={"ssl": {
                         "ssl_ca": "/etc/ssl/cert.pem",
                       }})

# with engine.connect() as conn:
#   result = conn.execute(text("select * from jobs "))
#   print("type(result:", type(result))
#   result_all = result.all()
#   print("type(result.all())", type(result.all))
#   #print("result.all()",result_all)
#   first_result = result_all[0]
#   print("type(first_result)", type(first_result))
#   first_result_dict = dict(result_all[0])
#   print("type of (first_result_dict)", type(first_result_dict))
#   print(first_result_dict)

# with engine.connect() as conn:
#   result = conn.execute(text("select * from jobs"))
#   result_all = result.all()
#   first_result = result_all[0]
#   column_names = result.keys()
#   first_result_dict = dict(zip(column_names, first_result))
#   print(first_result_dict)

# with engine.connect() as conn:
#   result = conn.execute(text("select * from jobs"))
#   column_names = result.keys()

#   result_dicts = []

#   for row in result.all():
#     result_dicts.append(dict(zip(column_names, row)))
#   print(result_dicts)


def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("select * from jobs"))
    column_names = result.keys()
    jobs = []
    for row in result.all():
      jobs.append(dict(zip(column_names, row)))
    return jobs
