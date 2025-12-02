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
    
    **Option A: Run the main script (batch mode)**
    ```bash
    docker-compose up spark --build
    ```
    This will build the Docker image and run the Spark application, executing all analyses and generating visualizations.
    
    **Option B: Run Jupyter Notebook (interactive mode)**
    ```bash
    docker-compose up jupyter --build
    # Or use the convenience script:
    ./start-jupyter.sh
    ```
    Then open your browser to http://localhost:8888 to access Jupyter Notebook.
    See [JUPYTER_SETUP.md](JUPYTER_SETUP.md) for detailed instructions.

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
    ├── analysis/           # Analysis modules
    ├── utils/             # Utility modules (reader, schemas)
    ├── visualizations/    # Generated visualization outputs
    ├── Dockerfile
    ├── docker-compose.yml
    ├── main.py           # Main entry point for batch execution
    ├── imdb_analysis.ipynb  # Jupyter notebook for interactive analysis
    ├── start-jupyter.sh   # Convenience script to start Jupyter
    ├── requirements.txt
    ├── JUPYTER_SETUP.md  # Detailed Jupyter setup instructions
    └── BUSINESS_QUESTIONS.md
```

## Key Files

-   `data/`: Contains the IMDb dataset files (TSV format)
-   `analysis/`: Python modules with pre-built analyses:
    -   `actor_analysis.py` - Actor demographics and career statistics
    -   `director_analysis.py` - Director trends and collaborations
    -   `genre_analysis.py` - Genre popularity and evolution
    -   `tv_analysis.py` - TV series ratings and seasons
    -   `collaboration_analysis.py` - Writer/director partnerships
    -   `localization_analysis.py` - Regional distribution patterns
-   `main.py`: Batch execution script for running all analyses
-   `imdb_analysis.ipynb`: Interactive Jupyter notebook
-   `docker-compose.yml`: Defines two services:
    -   `spark`: Runs main.py for batch processing
    -   `jupyter`: Runs Jupyter Notebook server on port 8888
-   `Dockerfile`: Docker image with Python 3.9, Java 17, and PySpark 4.0

## Interactive Analysis with Jupyter

The project includes a Jupyter Notebook (`imdb_analysis.ipynb`) for interactive data exploration:

1. Start Jupyter: `./start-jupyter.sh` or `docker-compose up jupyter`
2. Open http://localhost:8888 in your browser
3. Run analyses interactively, modify queries, and create custom visualizations

See [JUPYTER_SETUP.md](JUPYTER_SETUP.md) for more details.
