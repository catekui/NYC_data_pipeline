# Python Data Pipeline

A streaming data pipeline using Python with Kafka, Spark, PostgreSQL, and Streamlit.  

Clone the repository with:

`git clone https://github.com/your-username/data-pipeline.git`  
and move into the project folder with:

`cd data-pipeline`

Create a folder named `data` and add the dataset files to it. Once the dataset is in place, start the pipeline using Docker Compose:

`docker-compose up --build`

The Streamlit dashboard will be available at:

`http://localhost:8501`

You can follow logs for each container using:

`docker-compose logs -f`



