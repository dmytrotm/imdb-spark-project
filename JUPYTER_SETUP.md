# Jupyter Notebook Setup

This guide explains how to run the IMDb analysis project using Jupyter Notebook through Docker.

## Starting Jupyter Notebook

To start the Jupyter Notebook server:

```bash
docker-compose up jupyter
```

This will:
- Build the Docker image with all dependencies (including Jupyter)
- Start the Jupyter Notebook server
- Expose it on port 8888

## Accessing Jupyter

Once the container is running, you'll see output similar to:

```
jupyter-1  | [I 16:30:00.000 NotebookApp] Serving notebooks from local directory: /app
jupyter-1  | [I 16:30:00.000 NotebookApp] Jupyter Notebook 6.x.x is running at:
jupyter-1  | [I 16:30:00.000 NotebookApp] http://0.0.0.0:8888/
```

Open your browser and navigate to:

```
http://localhost:8888
```

**Note:** Authentication is disabled for convenience in local development.

## Available Notebooks

- `imdb_analysis.ipynb` - Main interactive notebook with examples of:
  - Loading IMDb datasets
  - Running pre-built analyses
  - Creating custom queries
  - Generating visualizations

## Running the Main Script vs Jupyter

You have two options:

### Option 1: Run the main script (batch mode)
```bash
docker-compose up spark
```

This will execute `main.py` and generate all visualizations automatically.

### Option 2: Use Jupyter (interactive mode)
```bash
docker-compose up jupyter
```

This allows you to:
- Run analyses step by step
- Modify queries interactively
- Explore the data
- Create custom visualizations

## Tips

1. **Memory Settings**: Both services are configured with 4GB driver and executor memory. Adjust in `docker-compose.yml` if needed:
   ```yaml
   environment:
     - SPARK_DRIVER_MEMORY=4g
     - SPARK_EXECUTOR_MEMORY=4g
   ```

2. **Notebooks are Persistent**: Any notebooks you create or modify are saved to your local machine (mounted volume).

3. **Visualizations**: All generated visualizations are saved to the `visualizations/` folder.

4. **Stopping the Server**: Press `Ctrl+C` in the terminal where docker-compose is running, or run:
   ```bash
   docker-compose down
   ```

## Example Workflow

1. Start Jupyter:
   ```bash
   docker-compose up jupyter
   ```

2. Open http://localhost:8888 in your browser

3. Open `imdb_analysis.ipynb`

4. Run cells sequentially (Shift+Enter) or run all (Cell → Run All)

5. Modify queries and re-run as needed

6. View generated visualizations in the `visualizations/` folder

## Troubleshooting

- **Port already in use**: If port 8888 is already in use, modify the port mapping in `docker-compose.yml`:
  ```yaml
  ports:
    - "9999:8888"  # Use port 9999 instead
  ```

- **Out of memory errors**: Increase memory settings in `docker-compose.yml`

- **Data not found**: Ensure the data folder is properly mounted and contains the `.tsv` files

