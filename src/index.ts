import { S3Client, GetObjectCommand, ListObjectsV2Command, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from 'fs';
import csv from 'csv-parser';
import { promisify } from 'util';
import stream from 'stream';
import { format } from 'fast-csv';

// Environnement variables configuration
import dotenv from 'dotenv';
import { env } from "process";
dotenv.config();

if(!env.AWS_ACCESS_KEY_ID || !env.AWS_SECRET_ACCESS_KEY || !env.AWS_DEFAULT_REGION) {
    throw new Error('AWS credentials are missing');
}

if(!env.S3_BUCKET_NAME) {
    throw new Error('Bucket name is missing');
}

// S3 Configuration
const s3Client = new S3Client({
    region: env.AWS_DEFAULT_REGION,
    credentials: {
        accessKeyId: env.AWS_ACCESS_KEY_ID,
        secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    }
});

interface SegmentResult {
  url: string;
  segments: string[];
}

const pipeline = promisify(stream.pipeline);

async function downloadFileFromS3(bucket: string, key: string, localPath: string): Promise<void> {
    const getObjectParams = {
        Bucket: bucket,
        Key: key
    };

    const data = await s3Client.send(new GetObjectCommand(getObjectParams));
    if (data.Body) {
                const readableStream = data.Body as stream.Readable; // Conversion explicite
                await pipeline(readableStream, fs.createWriteStream(localPath));
            }
}

async function listAllFiles(bucketName: string): Promise<string[]> {
    const listObjectsParams = {
        Bucket: bucketName
    };

    const data = await s3Client.send(new ListObjectsV2Command(listObjectsParams));
    return data.Contents?.map(file => file.Key!) || [];
}

export async function processCsvFile(filePath: string, segmentMappings: Record<number, string>): Promise<SegmentResult[]> {
    const results: SegmentResult[] = [];

    await pipeline(
        fs.createReadStream(filePath),
        csv({
            separator: ';', // Specify the CSV separator
            headers: ['url', 'segmentData'], // Specify the CSV headers
        })
        .on('data', (row) => {
            // The first column is the URL, the second column is the segment data
            const url = row.url;
            const rawSegmentData = row.segmentData as string;
            if (!rawSegmentData) return; // Ignorez les lignes sans données de segment

            const segments = rawSegmentData.split(',')
                .map(s => s.split('='))
                .filter(([_id, score]) => parseInt(score) >= 500)
                .map(([id, _score]) => segmentMappings[parseInt(id)])
                .filter(name => name); // Filtrer les noms non trouvés dans segmentMappings

            if (segments.length > 0) {
                results.push({ url, segments });
            }
        })
    );

    return results;
}

function createCsvFile(data: any[], filePath: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const csvStream = format({ headers: true });
        const writableStream = fs.createWriteStream(filePath);

        writableStream.on('finish', resolve);
        writableStream.on('error', reject);

        csvStream.pipe(writableStream);

        data.forEach(row => {
            // Convert each row to the expected format
            const csvRow: any = { url: row.url };
            row.segments.forEach((segment: string, index: number) => {
                csvRow[`segment${index + 1}`] = segment;
            });
            csvStream.write(csvRow);
        });

        csvStream.end();
    });
}

async function uploadFileToS3(bucket: string, key: string, localPath: string): Promise<void> {
    // Read content from the file and upload it to the S3 bucket
    const fileContent = fs.readFileSync(localPath);

    const putObjectParams = {
        Bucket: bucket,
        Key: key,
        Body: fileContent
    };

    await s3Client.send(new PutObjectCommand(putObjectParams));
}



async function run() {
    const bucketName = env.S3_BUCKET_NAME as string;

    try {
        // List all files in the bucket
        const allFiles = await listAllFiles(bucketName);

        // Download the segments.json file and parse it
        const segmentsLocalPath = './files/segments.json';
        await downloadFileFromS3(bucketName, 'segments.json', segmentsLocalPath);
        const segmentsArray: { id: number; name: string; }[] = JSON.parse(fs.readFileSync(segmentsLocalPath, 'utf8'));
        const segmentMappings = segmentsArray.reduce((acc, segment) => {
            acc[segment.id] = segment.name;
            return acc;
        }, {} as Record<number, string>);

        // Filter the CSV files
        const csvFiles = allFiles.filter(file => !file.includes('/') && file.endsWith('.csv'));
        for (const file of csvFiles) {
            const localCsvPath = `./files/${file}`;
            await downloadFileFromS3(bucketName, file, localCsvPath);
            const processedData = await processCsvFile(localCsvPath, segmentMappings);
            console.log(`Processed data for ${file}`);
                // Chemin où le nouveau fichier CSV sera enregistré
            const newCsvFilePath = './files/processed_data.csv';

            // Create the new CSV file 
            await createCsvFile(processedData, newCsvFilePath);

            // Download the new CSV file to S3
            const newCsvKey = `submissions/david_fradel-2023/${file}`;
            await uploadFileToS3(bucketName, newCsvKey, newCsvFilePath);

            console.log('CSV file processed and uploaded successfully.');
        }
        console.log('All files processed successfully ! 👍 👍 👍');
        process.exit();
    } catch (error) {
        console.error('Error:', error);
    }
}

run();