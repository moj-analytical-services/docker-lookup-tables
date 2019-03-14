# docker-lookup-tables

Docker image for creating lookup tables
=======================================

Docker image to parse data from files in github and add them to a data store in lookup tables


Usage
=====

Create a repo with name prefixed by `lookup-...`

Add a `deploy.json` file in the top level of your repo containing:

.. code:: JSON

    {
      "type": "lookup"
    }

Create a `./data` and `./meta` directory in the top level of the repo

Add a database.json database schema to the `./meta` directory - description of file here: `https://github.com/moj-analytical-services/etl_manager <https://github.com/moj-analytical-services/etl_manager>`_


..  code:: JSON

    {
        "description": "Database description ...",
        "name": "{code_db_name - same as repo name minus the lookup-}",
        "bucket": "moj-analytics-lookup-tables",
        "base_folder": "database/{code_db_name}"
    }


Add a .csv file for each table named `{table_name}.csv` where `table_name` is the name of the table you want to create

Add a .json table schema file for each table named `{table_name}.json` where `table_name` is the name of the table you want to create and is the same name as the csv file. For information on the table schema file see here: `https://github.com/moj-analytical-services/etl_manager <https://github.com/moj-analytical-services/etl_manager>`_

Create a release and concourse should add a job that will create a new database and add the csv data from each .csv file in to a table

Testing
=======

You can test the structure of your lookup repo with the following docker command:

.. code:: bash

    docker run \
        -e SOURCE_DIR=/source \
        -v data:/source/data \
        -v meta:/source/meta \
        --entrypoint "" \
        quay.io/mojanalytics/lookup-tables-etl:latest \
        pytest /tests/test_data.py
