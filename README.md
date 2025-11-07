# Big Data Analysis of IMDb Dataset using Apache Spark

## Prerequisites

- Docker
- Docker Compose

## Getting Started

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/dmytrotm/imdb-spark-project.git
    cd imdb-spark-project
    ```

2.  **Download the IMDb dataset:**
    Download the following datasets from [IMDb Datasets](https://datasets.imdbws.com/) and place them in the `data` directory at the root of the project (next to the `imdb-spark-project` directory):
    - `name.basics.tsv.gz`
    - `title.akas.tsv.gz`
    - `title.basics.tsv.gz`
    - `title.crew.tsv.gz`
    - `title.episode.tsv.gz`
    - `title.principals.tsv.gz`
    - `title.ratings.tsv.gz`

    You will need to decompress the files using `gunzip`.

3.  **Build and run the Spark application:**
    From within the `imdb-spark-project` directory, run the following command:
    ```bash
    docker-compose up --build
    ```
    This will build the Docker image and run the Spark application. You should see the output of the Spark job in your terminal, including the schema and the first 5 rows of the `title.ratings` DataFrame.

## Project Structure

```
.
├── data/
│   ├── name.basics.tsv
│   ├── title.akas.tsv
│   ├── title.basics.tsv
│   ├── title.crew.tsv
│   ├── title.episode.tsv
│   ├── title.principals.tsv
│   └── title.ratings.tsv
└── imdb-spark-project/
    ├── .gitignore
    ├── Dockerfile
    ├── docker-compose.yml
    ├── main.py
    ├── reader.py
    ├── requirements.txt
    └── schemas.py
```

-   `data/`: This directory contains the IMDb dataset files.
-   `imdb-spark-project/`: This directory contains the Spark application code.
    -   `Dockerfile`: Defines the Docker image for the Spark application.
    -   `docker-compose.yml`: Defines the services, networks, and volumes for the Docker application.
    -   `main.py`: The main entry point for the Spark application.
    -   `reader.py`: A module to read the data from the TSV files into Spark DataFrames.
    -   `requirements.txt`: The Python dependencies for the project.
    -   `schemas.py`: Defines the schemas for the IMDb datasets.
