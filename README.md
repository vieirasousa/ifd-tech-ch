# iFood - Technical Challenge

### Overview

###### This project is part of a hiring process for iFood, ocurring between November and December 2024.
-------------------------------------------------------------------------
## Case requirements

This technical case has the following requirements:

1. Create a solution to ingest raw data from https://www.nyc.gov/site/tlc/about/tlc-
trip-record-data.page, with the following guidelines:
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
    1.  What's the average (total\_amount) value received per month?
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

Information about the environment needed to developed and run this project, the architecture and decisions made can be found on separated docs (**environment.md** and **architecture.md**).
