# S3Transformer Project Documentation

## Overview

S3Transformer is a Node.js application designed to process CSV files stored in an Amazon S3 bucket, filter data based on specific criteria, and generate a new CSV file that is subsequently uploaded to the same S3 bucket.

## Prerequisites

- Node.js (version 14 or higher recommended)
- npm (usually comes with Node.js)
- An AWS account with access to Amazon S3
- AWS Configuration (access keys set up via `.env` file or AWS CLI)

## Installation

1. **Clone the Git Repository** (or download the zip and extract):

```
    git clone https://your-git-repo-url.git

    cd S3Transformer
```

2. **Install Dependencies**:

```
    npm install
```

3. **Configure Environment Variables**:
Create a `.env` file at the root of the project with your AWS access keys and the name of the S3 bucket.

```
    AWS_ACCESS_KEY_ID=your_access_key_id
    AWS_SECRET_ACCESS_KEY=your_secret_access_key
    S3_BUCKET_NAME=your_bucket_name
```


## Usage

To run the application, use the following command:
```
npm start
```

This command will execute the main script which performs the transformation process on the CSV files.

## Testing

To run the tests:
```
npm run test
```
This will execute unit tests defined in the `tests` folder.

## Linting

To run the code linter and detect any style or syntax errors:
```
npm run lint
```


## Project Structure

- `src/`: Contains the source code of the application.
- `tests/`: Contains the unit tests.
- `node_modules/`: Contains installed third-party modules.
- `package.json`: Defines project metadata and dependencies.
- `tsconfig.json`: Configuration for the TypeScript compiler.
- `.env`: A file for setting environment variables (not tracked by Git for security).

## Error Handling

The application handles errors by logging them to the console. If processing a CSV file fails, that file is skipped, and the process continues with the next file.