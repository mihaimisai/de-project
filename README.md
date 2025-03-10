# <center>**DATA ENGINEERING FINAL PROJECT**</center>

## Project Summary:

The following directory acts as an Extract-Transform-Load (ETL) pipeline, transporting and re-structuring raw, unformatted data to Dimension and Fact tables, specifically in accordance with several predetermined, outlined Star Schemas. This is achieved primarily through the use of *Cloud Engineering*, *Data Engineering* and *Code as Infrastructure* principals and thus fundamentally relies on the commissioning of Amazon-Web-Services (AWS) resources utilising Terraform along-side fully tested and reviewed Python scripts. In addition, throughout the project, *Continuous Integration and Continuous Delivery* (CI/CD) was practiced, alongside *Test-Driven Development* (TTD) to maximise both the effectiveness and validity of such code, as written and deployed.
<center>
<img src="markdown-photos/mvp.png" width="500">
</center>

## Lambda One (Extraction):

The initial Lambda, a serverless computing service delivered by AWS, extracts raw, unformatted data from the *TOTESYS* PSQL database using pg8000, and dumps it into a specified s3 Ingestion Bucket (a cloud storage system delivered by AWS) as a .csv file, structured by tablename, year, month and day:

<center>

```python
f"{table_name}/{year}/{month}/{day}/{time_stamp}.csv"
```
</center>

The lambda utilises timestamp-based referencing to ascertain which data entries within the *TOTESYS* PSQL database are new, and which have already been extracted into the s3 Ingestion Bucket and thus unwanted data-duplication is prohibited.

## Lambda Two (Transformation):

The second Lamba transforms all extracted data from the s3 Ingestion Bucket, specifically in accordance with the following Star Schemas, as outlined below:

<center>
<img src="markdown-photos/star-schemas.png" width="500">
</center>

This is accomplished, primarily, through leveraging the Pandas module and manipulating / re-structuring the raw, unformatted data into an informed and readable format. Such data is then outputted within *parquet* format to a second s3 Bucket, specifically for processed data, otherwise termed 'Transformation Bucket'.  

## Lambda Three (Load):

The third and final Lambda is responsible for loading the transformed data from the s3 Transformation Bucket into a Redshift Data Warehouse, hosted by AWS. 

<center>

```python
"insert warehouse overview screenshot here."
```
</center>

## Extra Features:

#### Cloudwatch Logging:
- Multi-tier logging is implemented throughout the lambda scripts and varying other infrastructure-related resources, as deployed via Terraform. Collected via AWS's Cloudwatch, such messages log error- and success- related information specifically detailing the condition of the entire ETL pipeline, start to finish.

#### Email Alerts:
- In the event that the aforementioned Cloudwatch Logging System encounters an error, an informative message is delivered to a shared email address- "NotificationStreamBanshee@Gmail.com"- allowing for quick resolution by the DevOps team:

<center>
<img src="markdown-photos/email-alert.png" width="500">
</center>

#### State-Machine and Scheduling:
- All lambdas exist within the scope of AWS's State-Machine, a event-driven workflow allowing seemless integration between all of the cloud infrastructure, as deployed within this repository. The inital lambda is triggered, and thus begins ingesting data from the *TOTESYS* database every 20 minutes. Both subsequent Lambdas (both Transformation and Load) are trigger to execute when new data is dumped into the s3 Bucket that proceeds them within the State-Machine, consequently allowing for the data to be successfully transported down the ETL pipeline. 

<center>
<img src="markdown-photos/state-machine.png" width="100">
</center>


#### Lambda Dependencies:
- All of the Lambda dependencies exist within a designated Dependencies/Python file, within which additional Python packages and modules not innately supported by AWS's serverless computing are deployed. For this reason, any changes to the requirements.txt file within this repository much be reflected in the dependencies section to ensure no ModuleNotFound errors are incorrectly raised. 

#### CI/CD, Github Actions and .yml:
- The build, test and deploy pipeline central to the development of this repositiory was automated through Github Actions in accordance to our *"test-and-deploy.yml"* file. This ensured that all scripts pushed to this repository underwent stringent validation prior to being incorporated and deployed. This ensured all Python scripts were fully PEP8 compliant through leveraging Flake8 and Black modules, passed all respective unittests and furthermore meant that all Terraform-deployed AWS resources were valid and well-formed. 

#### State Bucket:
- The Terraform state file, utilised by Terraform to map real-world resources to your configuration, keep track of metadata, and improve performance for large infrastructures, is stored in a seperate s3 Bucket, deployed via the AWS console. This allows for cross-platform collaboration between the our team and prevents issues with non-centralised modifications to the Terraform state. 


#### Created by Team Banshee:
- Shea Macfarlane.
- Mihai Misai. 
- Meral Hewitt. 
- Anna Fedyna.
- Carlo Danieli. 
- Ahmad Fadhli.


