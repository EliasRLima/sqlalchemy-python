import pandas as pd
import sqlalchemy as db

#engine = db.create_engine('dialect+driver://user:pass@host:port/db')
engine = db.create_engine('sqlite:///census.sqlite')
connection = engine.connect()
metadata = db.MetaData()
census = db.Table('census', metadata, autoload=True, autoload_with=engine)
print(census.columns.keys())
#print(repr(metadata.tables['census']))

query = db.select([census]) #Equivalent to 'SELECT * FROM census'
ResultProxy = connection.execute(query)
ResultSet = ResultProxy.fetchall()
ResultSet[:3]

#Filtering
#SELECT * FROM census WHERE sex = F
query = db.select([census]).where(census.columns.sex == 'F')
#SELECT state, sex FROM census WHERE state IN (Texas, New York)
query = db.select([census.columns.state, census.columns.sex]).where(census.columns.state.in_(['Texas', 'New York']))
#SELECT * FROM census WHERE state = 'California' AND NOT sex = 'M'
query = db.select([census]).where(db.and_(census.columns.state == 'California', census.columns.sex != 'M'))
#SELECT * FROM census ORDER BY State DESC, pop2000
query = db.select([census]).order_by(db.desc(census.columns.state), census.columns.pop2000)
#SELECT SUM(pop2008) FROM census
query = db.select([db.func.sum(census.columns.pop2008)])
#SELECT SUM(pop2008) as pop2008, sex FROM census group by sex
query = db.select([db.func.sum(census.columns.pop2008).label('pop2008'), census.columns.sex]).group_by(census.columns.sex)
#SELECT DISTINCT state FROM census
query = db.select([census.columns.state.distinct()])


#cast
female_pop = db.func.sum(db.case([(census.columns.sex == 'F', census.columns.pop2000)], else_=0))
total_pop = db.cast(db.func.sum(census.columns.pop2000), db.Float)
query = db.select([female_pop/total_pop * 100])
result = connection.execute(query).scalar()
#print(result)

#Join
state_fact = db.Table('state_fact', metadata, autoload=True, autoload_with=engine)
print(state_fact.columns.keys())

query = db.select([census, state_fact])
query = query.select_from(census.join(state_fact, census.columns.state == state_fact.columns.name))
results = connection.execute(query).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(5)

#Creating Database and Table
engine = db.create_engine('sqlite:///test.sqlite') #Create test.sqlite automatically
connection = engine.connect()
metadata = db.MetaData()

emp = db.Table('emp', metadata,
              db.Column('Id', db.Integer()),
              db.Column('name', db.String(255), nullable=False),
              db.Column('salary', db.Float(), default=100.0),
              db.Column('active', db.Boolean(), default=True)
              )

metadata.create_all(engine) #Creates the table

#Inserting Data
#one by one
query = db.insert(emp).values(Id=1, name='naveen', salary=60000.00, active=True)
ResultProxy = connection.execute(query)
#many records at ones
query = db.insert(emp)
values_list = [{'Id':'2', 'name':'ram', 'salary':80000, 'active':False},
               {'Id':'3', 'name':'ramesh', 'salary':70000, 'active':True}]
ResultProxy = connection.execute(query, values_list)

results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

#Updating data in Databases
#db.update(table_name).values(attribute = new_value).where(condition)

# Build a statement to update the salary to 100000
query = db.update(emp).values(salary=100000)
query = query.where(emp.columns.Id == 1)
results = connection.execute(query)

results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

#Delete Table
#db.delete(table_name).where(condition)

# Build a statement to delete where salary < 100000
query = db.delete(emp)
query = query.where(emp.columns.salary < 100000)
results = connection.execute(query)

results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)


#Dropping a Table

emp.drop(engine) #drops a single table
metadata.drop_all(engine) #drops all the tables in the database
