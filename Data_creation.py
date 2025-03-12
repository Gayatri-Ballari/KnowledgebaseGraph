import pandas as pd
from neo4j import GraphDatabase

# Sample Dataset Creation
def create_sample_datasets():
    # Movies table
    movies_data = {
        'movie_id': list(range(1, 101)),
        'title': [f'Movie {i}' for i in range(1, 101)],
        'year': [2000 + (i % 20) for i in range(1, 101)],
        'director_id': [(i % 10) + 1 for i in range(1, 101)]
    }
    # Actors table
    actors_data = {
        'actor_id': list(range(1, 101)),
        'name': [f'Actor {i}' for i in range(1, 101)]
    }
    # Movie-Actor relationships table
    movie_actors_data = {
        'movie_id': [i for i in range(1, 101) for _ in range(3)],
        'actor_id': [(i % 100) + 1 for i in range(1, 301)]
    }
    # Directors table
    directors_data = {
        'director_id': list(range(1, 11)),
        'name': [f'Director {i}' for i in range(1, 11)]
    }
    # Genres table
    genres_data = {
        'genre_id': list(range(1, 11)),
        'name': [f'Genre {i}' for i in range(1, 11)]
    }
    # Movie-Genre relationships table
    movie_genres_data = {
        'movie_id': [i for i in range(1, 101) for _ in range(2)],
        'genre_id': [(i % 10) + 1 for i in range(1, 201)]
    }
    return {
        'movies': pd.DataFrame(movies_data),
        'actors': pd.DataFrame(actors_data),
        'movie_actors': pd.DataFrame(movie_actors_data),
        'directors': pd.DataFrame(directors_data),
        'genres': pd.DataFrame(genres_data),
        'movie_genres': pd.DataFrame(movie_genres_data)
    }

class Neo4jKnowledgeGraph:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
    
    def create_constraints(self):
        with self.driver.session() as session:
            # Create constraints for unique IDs
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.movie_id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Actor) REQUIRE a.actor_id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Director) REQUIRE d.director_id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (g:Genre) REQUIRE g.genre_id IS UNIQUE")
    
    def create_movie_nodes(self, movies_df):
        with self.driver.session() as session:
            for _, row in movies_df.iterrows():
                session.run("""
                    CREATE (m:Movie {
                        movie_id: $movie_id,
                        title: $title,
                        year: $year
                    })
                """, dict(row))
    
    def create_actor_nodes(self, actors_df):
        with self.driver.session() as session:
            for _, row in actors_df.iterrows():
                session.run("""
                    CREATE (a:Actor {
                        actor_id: $actor_id,
                        name: $name
                    })
                """, dict(row))
    
    def create_director_nodes(self, directors_df):
        with self.driver.session() as session:
            for _, row in directors_df.iterrows():
                session.run("""
                    CREATE (d:Director {
                        director_id: $director_id,
                        name: $name
                    })
                """, dict(row))
    
    def create_genre_nodes(self, genres_df):
        with self.driver.session() as session:
            for _, row in genres_df.iterrows():
                session.run("""
                    CREATE (g:Genre {
                        genre_id: $genre_id,
                        name: $name
                    })
                """, dict(row))
    
    def create_relationships(self, movies_df, movie_actors_df, movie_genres_df):
        with self.driver.session() as session:
            # Create ACTED_IN relationships
            for _, row in movie_actors_df.iterrows():
                session.run("""
                    MATCH (a:Actor {actor_id: $actor_id})
                    MATCH (m:Movie {movie_id: $movie_id})
                    CREATE (a)-[:ACTED_IN]->(m)
                """, dict(row))
            # Create DIRECTED relationships
            for _, row in movies_df.iterrows():
                session.run("""
                    MATCH (d:Director {director_id: $director_id})
                    MATCH (m:Movie {movie_id: $movie_id})
                    CREATE (d)-[:DIRECTED]->(m)
                """, dict(row))
            # Create IN_GENRE relationships
            for _, row in movie_genres_df.iterrows():
                session.run("""
                    MATCH (m:Movie {movie_id: $movie_id})
                    MATCH (g:Genre {genre_id: $genre_id})
                    CREATE (m)-[:IN_GENRE]->(g)
                """, dict(row))

def main():
    # Create sample datasets
    datasets = create_sample_datasets()
    
    # Initialize Neo4j connection
    # Replace with your Neo4j credentials
    graph = Neo4jKnowledgeGraph(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="Atharv@123"
    )
    
    try:
        # Clear existing data
        graph.clear_database()
        
        # Create constraints
        graph.create_constraints()
        
        # Create nodes
        graph.create_movie_nodes(datasets['movies'])
        graph.create_actor_nodes(datasets['actors'])
        graph.create_director_nodes(datasets['directors'])
        graph.create_genre_nodes(datasets['genres'])
        
        # Create relationships
        graph.create_relationships(
            datasets['movies'],
            datasets['movie_actors'],
            datasets['movie_genres']
        )
        
        print("Knowledge graph created successfully!")
    
    finally:
        graph.close()

if __name__ == "__main__":
    main()