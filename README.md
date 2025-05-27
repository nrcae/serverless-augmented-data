# Serverless AI Dataset Insight Tool

This project implements a serverless tool using AWS Lambda to automatically generate insights from uploaded datasets using the OpenAI API. 
The augmented dataset (original data + AI insights) is then stored in an S3 bucket.

## Features

* **Serverless Architecture**: Uses AWS Lambda for event-driven processing.
* **S3 Integration**: Triggers on file uploads to S3 and stores results in another S3 bucket.
* **AI-Powered Insights**: Leverages the OpenAI API to generate insights from data.
* **Data Augmentation**: Combines original data with AI-generated insights.
* **Minimal & Extendable**: Provides a basic framework that can be extended for various data types and insight generation strategies.

## File Structure
```
dataset_insight_tool/
├── lambda_function.py       # Main Lambda handler
├── config.py                # Environment variables
├── utils                    # Utilities
    ├── openai_utils.py      # OpenAI API interaction
    ├── data_utils.py        # Data processing and augmentation
├── terraform                # Terraform configuration files
    ├── main.tf              # Core AWS resources
    ├── variables.tf         # Configuration of Variables
    ├── outputs.tf           # Output configuration
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Prerequisites

* AWS Account
* OpenAI API Key
* Python 3.13+ (or as supported by AWS Lambda)
* AWS CLI configured (for deployment)
* AWS SAM CLI or Serverless Framework (optional, for easier deployment)

## Setup & Deployment

1.  **Clone the Repository (or create files locally)**

2.  **Configure Environment Variables for Lambda:**
    * `OPENAI_API_KEY`:  OpenAI API key.
    * `OUTPUT_BUCKET_NAME`: The name of the S3 bucket where augmented datasets will be stored.
    * `OUTPUT_FORMAT`: Desired output format. Options: `json` (default), `csv`, `parquet`. Optional.
    * `TEXT_COLUMN_NAME`: Name of the column that contains the primary text for analysis (e.g., 'review_text', 'comment'). Defaults to 'text_column', then 'text_content' (for .txt files), then tries the first string field if not found. Optional.
    * `PROMPT_STRATEGY`: Defines how insights are generated. Options:
        * `per_record` (default): One insight per data record.
        * `summarize_all`: One overall insight for the entire dataset (based on concatenated text from records).
    * `MAX_CHARS_FOR_SUMMARY_PROMPT`: (Used with `summarize_all`) Maximum characters of combined text to send in a summary prompt. Default: `15000`. Optional.
    * `MAX_RECORDS_FOR_SUMMARY`: (Used with `summarize_all`) Maximum number of records to include in the combined text for summarization. Default: `100`. Optional.
    * `DYNAMODB_TABLE_NAME`: The name of the DynamoDB database where augmented datasets will be stored additionally.



3.  **Install Dependencies (for packaging, locally):**
    ```bash
    pip install -r requirements.txt -t ./package
    cp lambda_function.py openai_utils.py data_utils.py ./package/
    ```

4.  **Package for Lambda:**
    Create a ZIP file from the contents of the `package` directory.
    ```bash
    cd package
    zip -r ../lambda_deployment_package.zip .
    cd ..
    ```

5.  **Create Lambda Function:**
    * Go to AWS Lambda console.
    * Click "Create function".
    * Choose "Author from scratch".
    * **Function name**: e.g., `DatasetInsightTool`
    * **Runtime**: Python 3.x (e.g., Python 3.9)
    * **Architecture**: Choose appropriate (e.g., `x86_64`)
    * **Permissions**: Create new IAM role with basic Lambda permissions and attach policies to this role to allow:
        * Reading from the input S3 bucket (`s3:GetObject`).
        * Writing to the output S3 bucket (`s3:PutObject`).
        * Logging to CloudWatch Logs (`logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`).
    * Click "Create function".

6.  **Configure Lambda:**
    * **Code source**: Upload the `lambda_deployment_package.zip` file.
    * **Handler**: `lambda_function.lambda_handler` (filename.function_name) (usually automatically parsed upon function creation)
    * **Environment variables**: Add `OPENAI_API_KEY`, `OUTPUT_BUCKET_NAME`, and optionally `OUTPUT_FORMAT`, `TEXT_COLUMN_NAME`, `PROMPT_STRATEGY`, `MAX_CHARS_FOR_SUMMARY_PROMPT`, `MAX_RECORDS_FOR_SUMMARY`.
    * **Basic settings**: Adjust memory and timeout as needed. OpenAI calls can sometimes take a few seconds, so a timeout of 1 minute might be reasonable to start.
    * **Triggers**: Add an S3 trigger.
        * **Bucket**: Select input S3 bucket.
        * **Event type**: `All object create events` or more specific like `PUT`.
        * **Prefix/Suffix** (optional): If trigger only for files in a specific folder or with specific extensions is desired.

7.  **Test:**
    Upload CSV file (e.g., with a `text_column`) to folder in S3 bucket, check the CloudWatch logs for the Lambda function and the output S3 bucket as well as the DynamoDB for the augmented file.

