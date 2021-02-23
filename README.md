# Docker image for creating lookup tables

Docker image to parse data from files in github and add them to a data store in lookup tables


## Usage

Create a repo with name prefixed by `lookup_` *(making sure your repo name doesn't contain any dashes)*

Add a `deploy.json` file in the top level of your repo containing:

.. code:: JSON

    {
      "type": "lookup"
    }

Create a `./data` directory in the top level of the repo. This is where you will store your lookup tables in the following structure:

```
    ├── data/
    |   |
    │   ├── database_overwrite.json (optional, see below)
    |   |
    │   ├── lookup_table1/
    |   |   ├── data.csv
    |   |   ├── meta.json
    |   |   ├── README.md
    |   |
    │   ├── lookup_table2/
    |   |   ├── lookup_table2.csv
    |   |   ├── lookup_table2.json
```

Each folder in `data/` should be named after the lookup table that you want to deploy. Inside that lookup table folder you can add whatever you want in there (e.g. a README.md). But it must contain the following:

1. A csv file (your lookup table) which is either named `data` or has the same name as the directory it is in (i.e. the name off the lookup table). This csv should have a header.

2. A json file (your lookup table's metadata schema) which is either named `meta` or has the same name as the directory it is in (i.e. the name off the lookup table) For information on the table schema file see here: `https://github.com/moj-analytical-services/etl_manager <https://github.com/moj-analytical-services/etl_manager>`_

You do not need to provide a database json. This is inferred when the lookup database is deployed.


```json
{
    "description": "A lookup table deployed from {your lookup repo name}",
    "name": "{your lookup repo name}",
    "bucket": "moj-analytics-lookup-tables",
    "base_folder": "{your lookup repo name}/database"
}
```
You can set overides to these values by adding a `database_overwrite.json` to your `data/` folder. The values you can override are the `bucket` and the `description`. You may want to change the bucket to one that you control access to if you do not wish everyone in the organisation to be able to access the lookup table (which is the default). Note your s3 bucket must be prefixed with `alpha-lookup-`.

Create a release and concourse should add a job that will create a new database and add the csv data from each `.csv` file in to a table.

When concourse deploys your new lookup table you should see the following outputs:

- A new table partition in your lookup tables database where the partition is `release={github release}`
- your data and meta data folders (i.e. in the same structure as your lookup repository) in the s3 path `s3://moj-analytics-lookup-tables/{your lookup repo name}/{release}/` 

Note that the bucket will not be moj-analytics-lookup-tables if you speficied a different bucket in your `database_override.json`

These files in raw can be read directly from S3 if you do not wish to use the database versions of your lookup tables. 

## Running Locally

To build locally: 

```bash
docker build -t docker-lookup-tables:test . 
```

### Testing

You can test the structure of your lookup repo with the following docker command:

```bash
# you can point to your local build or directly to the image on the ECR instance
docker run \
    --entrypoint "" \
    docker-lookup-tables:test \
    pytest /tests/test_data.py
```

To just test on your own python:

```bash
python -m venv env
source env/bin/activate
pytest tests/
```

### Running manually

You will need to make sure you have AWS admin priveledges in your environment and have docker installed.

To ensure you're setup correctly please refer to [this guidance](https://github.com/moj-analytical-services/data-engineering/wiki/using-docker-and-ecr) on setting up Docker and ECR.

Clone their lookup repo and checkout the release they want to deploy. Once in the root dir of that repo and the correct release is checked out you can run:

```bash
docker run \
  -e RELEASE_TAG=$(git describe --tags) \
  -e GITHUB_REPO=$(basename `git rev-parse --show-toplevel`) \
  -e AWS_REGION=$AWS_REGION \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
  -v ${PWD}/data:/etl/data \
  593291632749.dkr.ecr.eu-west-1.amazonaws.com/docker-lookup-tables:<latest-release> # Note need to change latest-release
```
