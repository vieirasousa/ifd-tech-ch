# iFood - Technical Challenge

### Overview

###### This project is part of a hiring process for iFood, ocurring between November and December 2025.
-------------------------------------------------------------------------
## Case requirements

This technical case has the following requirements:

1. Create a solution to ingest raw data from https://www.nyc.gov/site/tlc/about/tlc-
trip-record-data.page , with the following guidelines:
  1. PySpark must be used at some point.
  2. Data must have metadata, using any chosen technology.
  3. Consider there's no tables on the Data Lake, so they will need to be modeled and created.
     
4. Extract only data from 2023, January to May, and write it to a Data Lake or similar structure.

5. Create a SQL layer over the extracted data to allow for business teams to query it. For this layer, the following columns must be present:
    1. **vendorID**
    2. **passenger\_count**
    3. **total\_amount**
    4. **tpep\_pickup\_datetime**
    5. **tpep\_dropoff\_datetime**
   
7. Using the extracted data answer the following questions, considering all the yellow taxis in the fleet:
    1. What's the average (total\_amount) value received per month?
    2. What's the average passenger count (passenger\_count) hourly, in May?

8. The code repository for this assignment must have the following structure:
```ifood-case/
  ├─ src/ # Código fonte da solução
  ├─ analysis/ # Scripts/Notebooks com as repostas das
  perguntas
  ├─ README.md
  └─ requirements.txt
```

10. Evaluation criteria
- Code quality and organization
- Exploratory analysis process
- Justification for the technical decisions.
- Creative effort on the proposed solution.
- Clarity while communicating the results.

Information about the environment needed to run this project, the components, the architecture and decisions made can be found on a separated doc, [architecture.md](https://github.com/vieirasousa/ifd-tech-ch/blob/main/architecture.md "About the architecture for this project").

As for the data analysis requirements, the script can be found on the *./analysis/* folder, and the required libraries for it are on the root *requirements.txt* file.

-------------------------------------------------------------------------
## How-to 

Follow the steps below in order to run this pipeline. It is assumed you already have created your ssh credentials and our public ssh key is registered on Github, but in case you don't, follow [this documentation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account).

1. In the cloud provider of your choice, create a Databricks instance and Workspace, with Data Warehouse, Unit Catalog capabilities and a pre-created usable Job Cluster.

2. In the sidebar on the left, select Workspace and then browse to the folder where you want to create the Git repo clone.

3. Click Add > Git folder.

4. On the Create Git folder dialog box, fill the required fields as below. The remaining fields can be left unfilled:

    * Git repository URL: git@github.com:vieirasousa/ifd-tech-ch.git
    * Git provider: GitHub
    * Git folder name: The name of the folder in your workspace that will contain the contents of the cloned repo

5. Now you have the repo cloned into your Databricks enviroment, click, on the left blade "Jobs & Pipelines".

6. Once in the Jobs & Pipelines area, click on the "Create" button. Define the first task, selecting for Type "Python script", loading "nyc_taxi_ingestion_yellow.py" on the "Path" field, giving it "ingestion_yellow_taxi" as Task name and click on "Save task". On the same screen, following this order, do the same for "nyc_taxi_ingestion_green.py" (changing the Task Name accordingly), for "nyc_taxi_silver.py" and for taxi_trips_by_vendor_date.py.silver

7. Give the Job a proper name clicking on its default name, right above the tabs "Runs" and "Tasks".

8. Click on "Run now" to trigger the Job and its tasks. As mentioned before, this pipeline should not be scheduled for recurrent runs without an additional resource to dynamically change "start_date" and "end_date" in the *config.json* file.