# docker-lookup-tables

Docker image for creating lookup tables
=======================================

Docker image to parse data from files in github and add them to a data store in lookup tables


Useage
======

Create a repo with name prefixed by `lookup-...`

Create a `./data` and `./meta` directory in the top level of the repo

Add a database.json database schema to the `./meta` directory - description of file here: `https://github.com/moj-analytical-services/etl_manager <https://github.com/moj-analytical-services/etl_manager>`_

Add a .csv file for each table named `{table_name}.csv` where `table_name` is the name of the table you want to create

Add a .json table schema file for each table named `{table_name}.json` where `table_name` is the name of the table you want to create and is the same name as the csv file. For information on the table schema file see here: `https://github.com/moj-analytical-services/etl_manager <https://github.com/moj-analytical-services/etl_manager>`_

Create a release and concourse should add a job that will create a new database and add the csv data from each .csv file in to a table
