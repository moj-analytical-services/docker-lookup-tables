Docker image for creating lookup tables
=======================================

Docker image to parse data from files in github and add them to a data store in lookup tables


Usage
=====

Create a repo with name prefixed by `lookup_` *(making sure your repo name doesn't contain any dashes)*

Add a `deploy.json` file in the top level of your repo containing:

.. code:: JSON

    {
      "type": "lookup"
    }

Create a `./data` and `./meta` directory in the top level of the repo

You do not need to provide a database json. This is inferred when the lookup database is deployed.

..  code:: JSON

    {
        "description": "A lookup table deployed from {your lookup repo name}",
        "name": "{your lookup repo name}",
        "bucket": "moj-analytics-lookup-tables",
        "base_folder": "{your lookup repo name}/database/"
    }

You can set overides to these values by adding a `database_overwrite.json` to your `meta/` folder. The values you can override are the `bucket` and the `description`. You may want to change the bucket to one that you control access to if you do not wish everyone in the organisation to be able to access the lookup table (which is the default).

Add a .csv file (with headers) for each table named `{table_name}.csv` where `table_name` is the name of the table you want to create

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
