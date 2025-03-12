import pandas as pd
from neo4j import GraphDatabase
from tabulate import tabulate
class MovieDBViewer:
   def __init__(self):
       self.uri = "bolt://localhost:7687"
       self.username = "neo4j"
       self.password = "Atharv@123"
       self.driver = None
   def connect(self):
       try:
           self.driver = GraphDatabase.driver(
               self.uri,
               auth=(self.username, self.password)
           )
           print("Connected to MovieDB successfully!")
           return True
       except Exception as e:
           print(f"Connection failed: {e}")
           return False
   def close(self):
       if self.driver:
           self.driver.close()
   def get_movies(self):
       query = """
       MATCH (m:Movie)
       RETURN m.movie_id as ID, m.title as Title, m.year as Year, m.rating as Rating
       ORDER BY m.movie_id
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
   def get_actors(self):
       query = """
       MATCH (a:Actor)
       RETURN a.actor_id as ID, a.name as Name, a.nationality as Nationality
       ORDER BY a.actor_id
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
   def get_directors(self):
       query = """
       MATCH (d:Director)
       RETURN d.director_id as ID, d.name as Name, d.nationality as Nationality
       ORDER BY d.director_id
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
   def get_genres(self):
       query = """
       MATCH (g:Genre)
       RETURN g.genre_id as ID, g.name as Name
       ORDER BY g.genre_id
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
   def get_movie_actors(self):
       query = """
       MATCH (a:Actor)-[:ACTED_IN]->(m:Movie)
       RETURN m.title as Movie, collect(a.name) as Actors
       ORDER BY m.title
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
   def get_movie_genres(self):
       query = """
       MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
       RETURN m.title as Movie, collect(g.name) as Genres
       ORDER BY m.title
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
   def get_movie_directors(self):
       query = """
       MATCH (d:Director)-[:DIRECTED]->(m:Movie)
       RETURN m.title as Movie, d.name as Director
       ORDER BY m.title
       """
       with self.driver.session() as session:
           result = session.run(query)
           return pd.DataFrame([dict(record) for record in result])
def display_table(df, title):
   print(f"\n{'-'*50}")
   print(f"{title}:")
   print(f"{'-'*50}")
   if df.empty:
       print("No data found")
   else:
       print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
   print()
def main():
   # First, install tabulate if not already installed
   try:
       import tabulate
   except ImportError:
       print("Installing required package: tabulate")
       import pip
       pip.main(['install', 'tabulate'])
   viewer = MovieDBViewer()
   try:
       if viewer.connect():
           # Get and display all tables
           display_table(viewer.get_movies(), "MOVIES")
           display_table(viewer.get_actors(), "ACTORS")
           display_table(viewer.get_directors(), "DIRECTORS")
           display_table(viewer.get_genres(), "GENRES")
           display_table(viewer.get_movie_actors(), "MOVIE-ACTOR RELATIONSHIPS")
           display_table(viewer.get_movie_genres(), "MOVIE-GENRE RELATIONSHIPS")
           display_table(viewer.get_movie_directors(), "MOVIE-DIRECTOR RELATIONSHIPS")
   except Exception as e:
       print(f"An error occurred: {e}")
   finally:
       viewer.close()
if __name__ == "__main__":
   main()