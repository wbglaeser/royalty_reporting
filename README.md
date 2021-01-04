# Weekly Royalty

The project is implemented using **python** and **airflow** and can be spun up with a simple docker-compose command.

As I did not have the required access to the google bigquery table the following is done assuming and inferring data formats from the task sheet.

To run and test the pipeline I have created a small number of mock up tables in my bigquery environment. The data goes back to the week starting on 2020-12-21. This is also when the airflow scheduler will start running.

Considering the size of the project the airflow setup comes with significant overhead. I decided for the approach both to allow for convenient monitoring and the possibility to scale at a later stage. The current versions runs locally using the LocalExecutor. For production this could of course be moved to a more suitable environment such as *google cloud compose*.

**Please Note**:

The dataset and tables are currently connected to my account. Please contact me for me to give access if needed.

### What happens

The pipeline follows a few chained steps that can be easily expanded, for example to include more tests.

**First**, the pipeline checks that the payout table *payout_yyyymmdd* for the running week exists. It does so using a BigQueryTableSensor that ships with Airflow. The table validation can easily be expanded to other tables.

**Second**, it matches the track_id from *listen_yyyymmdd* to a rightsholder_id from *track_rightsholder_rollup*. It does so while also validating that the rightsholder is valid at the time of the play.

Output Table :arrow_forward:  **weekly_track_rightsholder_yyyymmdd**

**Third**, it adds a track_title to the previous result using *track_information_rollup*. Seeing that the track_title is rather decorative and is not mentioned as a unique identifier in the final reporting table in the task sheet I simply assign the frist track_title that appears within the week for any given track_id.

Output Table :arrow_forward:  **weekly_track_title_rightsholder_yyyymmdd**

**Fourth**, it collects the total number of plays for a given rigthsholder id within a given week using *weekly_track_title_rightsholder_yyyymmdd* and *payout_yyyymmdd*. From that is computes the unit price for each rightsholder id. This does not allow for a single rightsholder to have multiple unit prices across different tracks. I comment on this below.

Output Table :arrow_forward:  **weekly_rightsholder_payout_yyyymmdd**

**Fifth**, it merges unit the unit price from rightsholder it on to *weekly_track_title_rightsholder_yyyymmdd* and does some renaming to produce the final reporting tool as described in the task sheet.

**Further information**:

* Weeks run from Monday to Sunday
* The script is run automatically every night
* No matter which day of the week it is the script recognises the running week which it identifies by the date of the first day of the week.
* When the script is first started it runs the pipeline for every week going back to a provided start date.



## Get up and running

1. Make sure you have [docker](https://docs.docker.com/engine/install/) and [docker-compose](https://docs.docker.com/compose/install/) installed on your system.

2. You need to setup your google environment. The details go beyond the scope here but generally the steps are.

  * create a google cloud project
  * enable the bigquery API for the project
  * create a service account for the project
  * create and download a key for the service account
  * rename and move the key to `/dags/config/credentials.json`

2. Using ssh, clone repo to local enviroment

  `git clone git@github.com:wbglaeser/royalty_reporting.git`

3. Navigate to repo directory and start up docker-compose

  `docker-compose up`

4. Go to Airflow WebUI which is now running at [localhost](http://localhost:8080/admin/).

5. Add google connection to connections

  a. Go to connections input form

  ![Navigation Tag](docs/admin.png)

  ![Connection Tag](docs/connections.png)

  b. Fill out input form with corresponding details and save

  ![Details Tag](docs/details.png)

6. Now you can start the airflow pipeline. It will start pulling data from the first week for which I have create mock data. The scheduler should now run the pipeline from up until today on a daily basis.

![Start Dag](docs/start_dag.png)


## Comments on the data

While setting up the reporting tool I came across some small questions. I outline here how I dealt with them.

* **multiple tracks on a rightsholder id**

  It was not entirely clear to me how to handle cases where a single rightsholder_id matches on multiple songs within a given week. The payout table does not differentiate by track_id for a rightsholder_id. In my solution I therefor assume that for given rightsholder_id the unit_price is identical for all tracks within a week.

* **multiple track_title on a track id**

  From the datasheet it seems possible that there can be multiple track_id, track_title and rightsholder_id combinations within a single reporting week. The final output table described in the task however only points to unique track_id and rightsholder_id combinations. The code therefor simply picks the first track_title for a given track_id within a week.

## Troubleshooting

* Issue with permissions

  If `docker-compose` gives you a permission error you need to adjust permissions for relevant subdirectories.

  `chmod -R 755 logs`
  `chmod -R 755 scripts`
